"""Backend abstraction for @spark.

Two kinds of backend, because prospects are asking for two genuinely different
execution models and forcing them into one shape produces a bad version of both:

``SESSION``
    A live ``SparkSession`` is handed to the step, which runs in the Metaflow task
    process. Spark operations execute elsewhere (a Databricks cluster via Spark
    Connect) or in-process (local Spark). There is no code to package and no
    serialization boundary, so this is the cheapest mode to make work correctly.

``SUBMIT``
    The user's Spark code is packaged, shipped to a remote compute service, and run
    there as a job. Needed for a pinned runtime, cluster-scoped libraries, JVM UDFs,
    or a driver that has to live on the cluster.

Backends only implement provider-specific calls. Polling, backoff, timeouts,
cancellation, and error classification live here so that every backend gets the same
behaviour and the same bugs get fixed once.
"""

import signal
import threading
import time
from contextlib import contextmanager

from ..context import JobState
from ..exceptions import (
    SparkControlPlaneError,
    SparkException,
    SparkJobCancelled,
    SparkJobFailed,
    SparkJobTimeout,
)

SESSION = "session"
SUBMIT = "submit"

POLL_INTERVAL_SECONDS = 5
STATUS_MESSAGE_INTERVAL_SECONDS = 60
CONTROL_PLANE_RETRIES = 6


class SparkBackend:
    #: registry key, e.g. "databricks"
    name = None
    #: SESSION or SUBMIT
    kind = SUBMIT

    def __init__(self, config, ctx=None):
        self.config = config
        self.ctx = ctx

    # ------------------------------------------------------------------
    # session backends
    # ------------------------------------------------------------------
    @contextmanager
    def session(self, ctx):
        """Yield a live SparkSession. Session backends must implement this."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # submit backends
    # ------------------------------------------------------------------
    def submit(self, ctx):
        """Package and submit the job. Returns a JobHandle."""
        raise NotImplementedError

    def poll(self, handle):
        """Return the current JobStatus. May raise to signal a control-plane fault."""
        raise NotImplementedError

    def cancel(self, handle):
        """Best-effort cancellation of a running job. Must not raise."""
        raise NotImplementedError

    def fetch_logs(self, handle, stream="stdout") -> "str | None":
        """Return the job's driver output on `stream`, or None if unavailable."""
        return None

    def stream_logs(self, handle):
        """Yield log lines while the job runs. Optional."""
        return iter(())

    def result(self, handle):
        """Return a reference to the job's output, or None."""
        return handle.output_url

    def read_output(self, handle, output_format):
        """Materialize the job's output in `output_format`."""
        return self.result(handle)

    def cost(self, handle):
        """Return a CostReport, or None if the backend cannot attribute cost."""
        return None

    def cleanup(self, handle, ctx):
        """Remove anything staged for the job. Must not raise."""

    # ------------------------------------------------------------------
    # shared driver
    # ------------------------------------------------------------------
    def wait(self, ctx, handle, timeout_minutes=None, poll_interval=None):
        """Poll until the job reaches a terminal state.

        Cancels the remote job on interrupt, timeout, or any unexpected exception, so
        that killing a flow does not leave compute running and billing.
        """
        poll_interval = poll_interval or POLL_INTERVAL_SECONDS
        deadline = None
        if timeout_minutes:
            deadline = time.monotonic() + timeout_minutes * 60

        last_message = 0.0
        last_state = None

        with _cancel_on_signal(self, ctx, handle):
            try:
                while True:
                    status = self._poll_with_retries(ctx, handle)

                    if status.state != last_state:
                        ctx.log("job state: %s" % status.state, job_id=handle.job_id)
                        last_message = time.monotonic()
                        last_state = status.state

                    if status.terminal:
                        return status

                    now = time.monotonic()
                    if deadline and now > deadline:
                        ctx.log(
                            "timeout of %d minutes exceeded, cancelling job"
                            % timeout_minutes,
                            job_id=handle.job_id,
                            stream="stderr",
                        )
                        self.safe_cancel(ctx, handle)
                        raise SparkJobTimeout(timeout_minutes, handle=handle)

                    if now - last_message > STATUS_MESSAGE_INTERVAL_SECONDS:
                        ctx.log(
                            _progress_message(status, deadline, now),
                            job_id=handle.job_id,
                        )
                        last_message = now

                    time.sleep(poll_interval)
            except (KeyboardInterrupt, SystemExit):
                ctx.log(
                    "interrupted, cancelling job", job_id=handle.job_id, stream="stderr"
                )
                self.safe_cancel(ctx, handle)
                raise
            except SparkException:
                # Already classified. Leave the job alone: a control-plane fault does
                # not mean the job is unhealthy, and cancelling blind would be worse.
                raise
            except Exception:
                self.safe_cancel(ctx, handle)
                raise

    def _poll_with_retries(self, ctx, handle):
        """Poll, tolerating transient control-plane failures.

        A provider API outage is reported as SparkControlPlaneError, never as a failed
        Spark job, so users are not sent to debug code that ran fine.
        """
        last_exc = None
        for attempt in range(CONTROL_PLANE_RETRIES):
            try:
                return self.poll(handle)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < CONTROL_PLANE_RETRIES - 1:
                    backoff = min(2**attempt, 30)
                    ctx.log(
                        "could not read job status (%s), retrying in %ds"
                        % (exc, backoff),
                        job_id=handle.job_id,
                        stream="stderr",
                    )
                    time.sleep(backoff)
        raise SparkControlPlaneError(
            "Failed to read the status of job %s after %d attempts. The job may still "
            "be running.\nLast error: %s%s"
            % (
                handle.job_id,
                CONTROL_PLANE_RETRIES,
                last_exc,
                "\nRun details: %s" % handle.ui_url if handle.ui_url else "",
            )
        ) from last_exc

    def safe_cancel(self, ctx, handle):
        try:
            self.cancel(handle)
            ctx.log("cancellation requested", job_id=handle.job_id)
        except Exception as exc:
            ctx.log(
                "failed to cancel job: %s. It may still be running and billing: %s"
                % (exc, handle.ui_url or handle.job_id),
                job_id=handle.job_id,
                stream="stderr",
            )

    def run(self, ctx, show_stdout=True, show_stderr=False, crash_on_failure=True):
        """Submit, wait, surface logs, and classify the outcome."""
        handle = self.submit(ctx)
        ctx.log(
            "submitted job%s" % (" (%s)" % handle.ui_url if handle.ui_url else ""),
            job_id=handle.job_id,
        )
        try:
            status = self.wait(ctx, handle, timeout_minutes=ctx.timeout_minutes)
        finally:
            if show_stdout:
                self._show_logs(ctx, handle, "stdout")
            if show_stderr:
                self._show_logs(ctx, handle, "stderr")

        if status.state == JobState.CANCELLED:
            raise SparkJobCancelled(
                "Spark job %s was cancelled.%s"
                % (handle.job_id, "\n%s" % handle.ui_url if handle.ui_url else "")
            )
        if not status.ok and crash_on_failure:
            raise SparkJobFailed(
                status, handle=handle, logs=self.fetch_logs(handle, "stderr")
            )
        return handle, status

    def _show_logs(self, ctx, handle, stream):
        try:
            text = self.fetch_logs(handle, stream)
        except Exception as exc:
            ctx.log(
                "could not retrieve %s: %s" % (stream, exc),
                job_id=handle.job_id,
                stream="stderr",
            )
            return
        if text:
            ctx.log(
                "job %s\n-----\n%s\n-----" % (stream, text.rstrip()),
                job_id=handle.job_id,
            )


def _progress_message(status, deadline, now):
    msg = "job state: %s" % status.state
    if status.stages:
        done = sum(s.num_completed or 0 for s in status.stages)
        total = sum(s.num_tasks or 0 for s in status.stages)
        if total:
            msg += " (%d/%d tasks)" % (done, total)
    if deadline:
        msg += ", %d min until timeout" % max(0, int((deadline - now) / 60))
    return msg


@contextmanager
def _cancel_on_signal(backend, ctx, handle):
    """Translate SIGINT/SIGTERM into remote cancellation plus the normal exception.

    Only installs handlers on the main thread; `signal.signal` is not usable
    elsewhere, and Metaflow may run steps off the main thread.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    previous = {}

    def handler(signum, frame):
        backend.safe_cancel(ctx, handle)
        old = previous.get(signum)
        if callable(old):
            old(signum, frame)
        elif signum == signal.SIGINT:
            raise KeyboardInterrupt()
        else:
            raise SystemExit(128 + signum)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[signum] = signal.signal(signum, handler)
        except (ValueError, OSError):
            previous.pop(signum, None)
    try:
        yield
    finally:
        for signum, old in previous.items():
            try:
                signal.signal(signum, old)
            except (ValueError, OSError):
                pass


# ----------------------------------------------------------------------
# registry
# ----------------------------------------------------------------------
# Lazily resolved so that importing this package never pulls in boto3, pyspark, or
# the Databricks SDK. Values are "module:attribute" relative to this package.
_BACKENDS = {
    "local": ".local:LocalSparkBackend",
    "databricks": ".databricks:DatabricksBackend",
    "databricks-connect": ".databricks:DatabricksConnectBackend",
    "databricks-jobs": ".databricks:DatabricksJobsBackend",
    "databricks-sql": ".databricks:DatabricksSqlBackend",
    "emr-serverless": ".emr_serverless:EMRServerlessBackend",
}

#: Accepted spellings for backend names.
_ALIASES = {
    "emr": "emr-serverless",
    "emr_serverless": "emr-serverless",
    "emrserverless": "emr-serverless",
    "dbx": "databricks",
    "databricks_connect": "databricks-connect",
    "connect": "databricks-connect",
    "databricks_jobs": "databricks-jobs",
    "databricks_sql": "databricks-sql",
    "sql-warehouse": "databricks-sql",
    "warehouse": "databricks-sql",
}


def register_backend(name, target):
    """Register a backend. `target` is a class or a "module:attribute" string."""
    _BACKENDS[name] = target


def available_backends():
    return sorted(_BACKENDS)


def get_backend_class(name):
    from importlib import import_module

    if not isinstance(name, str):
        raise SparkException(
            "@spark(backend=...) must be a string, got %r. Available backends: %s."
            % (name, ", ".join(available_backends()))
        )
    key = _ALIASES.get(name, name)
    target = _BACKENDS.get(key)
    if target is None:
        raise SparkException(
            "Unknown @spark backend '%s'. Available backends: %s."
            % (name, ", ".join(available_backends()))
        )
    if isinstance(target, str):
        module_path, _, attr = target.partition(":")
        module = import_module(module_path, package=__name__)
        target = getattr(module, attr)
        _BACKENDS[key] = target
    return target

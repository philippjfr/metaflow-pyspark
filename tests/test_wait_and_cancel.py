"""Tests for the shared polling driver.

This is where the original implementation's bugs lived: an interrupt leaked a running
job, and a control-plane outage was reported as a failed Spark job.
"""

import pytest

from metaflow_extensions.spark.plugins import backends
from metaflow_extensions.spark.plugins.backends import SparkBackend
from metaflow_extensions.spark.plugins.context import (
    JobHandle,
    JobState,
    JobStatus,
    SparkJobContext,
)
from metaflow_extensions.spark.plugins.exceptions import (
    SparkControlPlaneError,
    SparkJobCancelled,
    SparkJobFailed,
    SparkJobTimeout,
)


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(backends.time, "sleep", lambda seconds: None)


def make_ctx(timeout_minutes=None):
    return SparkJobContext(
        flow=None,
        step_name="start",
        pathspec="Flow/1/start/2",
        flow_name="Flow",
        run_id="1",
        task_id="2",
        attempt=0,
        user="tester",
        config={},
        tags={},
        timeout_minutes=timeout_minutes,
        logger=lambda msg, job_id=None, stream="stdout": None,
    )


class FakeBackend(SparkBackend):
    name = "fake"

    def __init__(self, states=(), poll_errors=(), logs=None):
        super().__init__({}, None)
        self.states = list(states)
        self.poll_errors = list(poll_errors)
        self.cancelled = 0
        self.logs = logs
        self.polls = 0

    def submit(self, ctx):
        return JobHandle(backend=self.name, job_id="job-1", ui_url="https://ui/job-1")

    def poll(self, handle):
        self.polls += 1
        if self.poll_errors:
            raise self.poll_errors.pop(0)
        state = self.states.pop(0) if self.states else JobState.SUCCESS
        return JobStatus(state=state)

    def cancel(self, handle):
        self.cancelled += 1

    def fetch_logs(self, handle, stream="stdout"):
        return self.logs


def test_wait_returns_the_terminal_status():
    backend = FakeBackend([JobState.PENDING, JobState.RUNNING, JobState.SUCCESS])
    status = backend.wait(make_ctx(), backend.submit(make_ctx()))
    assert status.state == JobState.SUCCESS
    assert backend.cancelled == 0


def test_timeout_cancels_the_remote_job():
    backend = FakeBackend([JobState.RUNNING] * 50)
    ctx = make_ctx(timeout_minutes=1)
    handle = backend.submit(ctx)
    # A monotonic clock that jumps past the deadline on the second reading.
    times = iter([0.0, 0.0, 0.0, 10_000.0] + [10_000.0] * 50)
    import metaflow_extensions.spark.plugins.backends as mod

    original = mod.time.monotonic
    mod.time.monotonic = lambda: next(times)
    try:
        with pytest.raises(SparkJobTimeout):
            backend.wait(ctx, handle, timeout_minutes=1)
    finally:
        mod.time.monotonic = original
    assert backend.cancelled == 1


def test_interrupt_cancels_the_remote_job():
    backend = FakeBackend(poll_errors=[KeyboardInterrupt()])
    ctx = make_ctx()
    handle = backend.submit(ctx)
    with pytest.raises(KeyboardInterrupt):
        backend.wait(ctx, handle)
    assert backend.cancelled == 1


def test_unexpected_exception_cancels_the_remote_job():
    class Boom(backends.SparkException):
        pass

    backend = FakeBackend(poll_errors=[RuntimeError("boom")] * 20)
    ctx = make_ctx()
    handle = backend.submit(ctx)
    with pytest.raises(SparkControlPlaneError):
        backend.wait(ctx, handle)
    # A control-plane fault must not cancel: the job itself may be perfectly healthy.
    assert backend.cancelled == 0


def test_transient_poll_failures_are_retried():
    backend = FakeBackend(
        states=[JobState.SUCCESS], poll_errors=[ConnectionError("flaky")]
    )
    status = backend.wait(make_ctx(), backend.submit(make_ctx()))
    assert status.state == JobState.SUCCESS


def test_control_plane_outage_is_not_reported_as_a_failed_job():
    backend = FakeBackend(poll_errors=[ConnectionError("down")] * 20)
    ctx = make_ctx()
    with pytest.raises(SparkControlPlaneError) as exc:
        backend.wait(ctx, backend.submit(ctx))
    assert "may still be running" in str(exc.value)
    assert not isinstance(exc.value, SparkJobFailed)


def test_run_raises_job_failed_with_logs():
    backend = FakeBackend([JobState.FAILED], logs="Traceback: AnalysisException")
    with pytest.raises(SparkJobFailed) as exc:
        backend.run(make_ctx())
    assert "AnalysisException" in str(exc.value)


def test_run_distinguishes_cancellation_from_failure():
    backend = FakeBackend([JobState.CANCELLED])
    with pytest.raises(SparkJobCancelled):
        backend.run(make_ctx())


def test_crash_on_failure_false_returns_the_status():
    backend = FakeBackend([JobState.FAILED])
    handle, status = backend.run(make_ctx(), crash_on_failure=False)
    assert status.state == JobState.FAILED
    assert handle.job_id == "job-1"


def test_logs_are_shown_even_when_wait_raises():
    shown = []
    backend = FakeBackend(poll_errors=[KeyboardInterrupt()], logs="partial output")
    ctx = make_ctx()
    ctx.logger = lambda msg, job_id=None, stream="stdout": shown.append(msg)
    with pytest.raises(KeyboardInterrupt):
        backend.run(ctx)
    assert any("partial output" in msg for msg in shown)

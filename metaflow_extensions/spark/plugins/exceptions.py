from metaflow.exception import MetaflowException


class SparkException(MetaflowException):
    headline = "Spark extension error"


class SparkConfigError(SparkException):
    headline = "Spark extension configuration error"


class SparkBackendUnavailable(SparkException):
    """Raised when a backend is selected but its dependencies are not installed."""

    headline = "Spark extension backend unavailable"

    def __init__(self, backend, package, extra=None):
        extra = extra or backend
        msg = (
            "The '%s' backend requires '%s', which is not installed.\n"
            "Install it with:\n"
            "    pip install metaflow-pyspark[%s]\n"
            "or add it to the step environment with @pypi(packages={'%s': ''})."
            % (backend, package, extra, package)
        )
        super().__init__(msg)


class SparkJobFailed(SparkException):
    """A remote statement or job reached a terminal failure state.

    Carries the backend's own error classification so the user does not have to go
    log spelunking to find out what happened.
    """

    headline = "Spark job failed"

    def __init__(self, status, handle=None, logs=None):
        self.status = status
        self.handle = handle
        lines = ["Spark job failed with state '%s'." % status.state]
        if status.error_class:
            lines.append("Error class: %s" % status.error_class)
        if status.message:
            lines.append("Message: %s" % status.message)
        if status.ui_url:
            lines.append("Run details: %s" % status.ui_url)
        if logs:
            tail = logs.strip().splitlines()[-40:]
            lines.append("\nLast %d lines of output:" % len(tail))
            lines.extend("    " + line for line in tail)
        super().__init__("\n".join(lines))


class SparkJobCancelled(SparkException):
    headline = "Spark job cancelled"


class SparkJobTimeout(SparkException):
    headline = "Spark job timed out"

    def __init__(self, timeout_minutes, handle=None):
        self.handle = handle
        msg = "Spark job did not finish within the %d minute timeout." % timeout_minutes
        if handle is not None and handle.ui_url:
            msg += "\nRun details: %s" % handle.ui_url
        super().__init__(msg)


class SparkControlPlaneError(SparkException):
    """The job may or may not be running: we lost the ability to ask.

    Deliberately distinct from SparkJobFailed so that a transient outage in the
    compute provider's API is never reported to the user as a failed job.
    """

    headline = "Lost contact with the Spark control plane"


class UnityCatalogError(SparkException):
    headline = "Unity Catalog error"

"""End-to-end exercise of @spark in session mode, with a stand-in for Spark.

Registering a fake backend here rather than requiring a real cluster tests the part that
is easy to get wrong and hard to notice: the decorator's interaction with Metaflow's task
lifecycle. In particular, a real SparkSession is not picklable, so if the session were
still attached to the flow when Metaflow persists artifacts, every step would fail.
"""

from contextlib import contextmanager

from metaflow import FlowSpec, current, spark, step

from metaflow_extensions.spark.plugins.backends import SESSION, register_backend
from metaflow_extensions.spark.plugins.backends import SparkBackend


class FakeSession:
    """Unpicklable on purpose, exactly like a real SparkSession."""

    version = "3.5.0-fake"

    def __init__(self):
        self.stopped = False
        self.tags = []

    def addTag(self, tag):
        self.tags.append(tag)

    def stop(self):
        self.stopped = True

    def __reduce__(self):
        raise TypeError("cannot pickle a SparkSession")


SESSIONS = []


class FakeSessionBackend(SparkBackend):
    name = "fake-session"
    kind = SESSION

    @contextmanager
    def session(self, ctx):
        session = FakeSession()
        session.addTag(ctx.job_name)
        SESSIONS.append(session)
        try:
            yield session
        finally:
            session.stop()


register_backend("fake-session", FakeSessionBackend)


class SessionFlow(FlowSpec):
    @spark(backend="fake-session")
    @step
    def start(self):
        assert current.spark is self.spark, "current.spark and self.spark must agree"
        self.version = self.spark.version
        self.tags = list(self.spark.tags)
        self.next(self.end)

    @step
    def end(self):
        # Reaching here at all proves the session was detached before persistence.
        assert self.version == "3.5.0-fake"
        assert not hasattr(self, "spark")
        assert len(self.tags) == 1
        assert self.tags[0].startswith("metaflow-SessionFlow-%s-start-" % current.run_id)
        print("session flow ok")


if __name__ == "__main__":
    SessionFlow()

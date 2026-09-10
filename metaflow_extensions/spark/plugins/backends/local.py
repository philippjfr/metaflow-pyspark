"""In-process Spark, so that a flow can be developed and tested without a cloud account.

This exists as much for CI as for laptops: it lets the demos and the test suite exercise
the whole @spark code path with no credentials, which is what makes the rest of the
backends safe to refactor.
"""

import shutil
import tempfile
from contextlib import contextmanager

from . import SESSION, SparkBackend
from ..exceptions import SparkBackendUnavailable


class LocalSparkBackend(SparkBackend):
    name = "local"
    kind = SESSION

    @contextmanager
    def session(self, ctx):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            raise SparkBackendUnavailable("local", "pyspark", extra="local")

        config = self.config
        master = config.get("master", "local[*]")
        params = dict(config.get("spark-parameters") or {})

        tmpdir = None
        if "spark.sql.warehouse.dir" not in params:
            tmpdir = tempfile.mkdtemp(prefix="metaflow-spark-")
            params["spark.sql.warehouse.dir"] = tmpdir

        builder = SparkSession.builder.master(master).appName(ctx.job_name)
        for key, value in params.items():
            builder = builder.config(key, str(value))

        session = builder.getOrCreate()
        ctx.log(
            "local Spark session ready (master=%s, version=%s)"
            % (master, session.version)
        )
        try:
            yield session
        finally:
            # Only stop a session we are responsible for. getOrCreate may have handed
            # back one the user created themselves, and stopping that would surprise.
            if config.get("stop_session", True):
                try:
                    session.stop()
                except Exception:
                    pass
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

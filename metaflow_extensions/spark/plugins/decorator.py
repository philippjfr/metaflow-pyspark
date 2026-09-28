"""The @spark step decorator."""

import functools
import json
import warnings

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

from .backends import SESSION, get_backend_class
from .config import backend_config, resolve_config
from .context import SparkJobContext, log
from .cost import build_tags
from .exceptions import SparkConfigError
from .output import from_spark_dataframe, strip_spark_attrs, validate_format

#: Attributes that describe the compute or the workspace rather than the step, and so
#: belong in the resolved backend's own config section.
_BACKEND_ATTRS = (
    "compute",
    "serverless",
    "cluster_id",
    "instance_pool_id",
    "num_workers",
    "node_type_id",
    "runtime_version",
    "photon",
    "dependencies",
    "libraries",
    "volume",
    "output_table",
    "output_path",
    "write_mode",
    "partition_by",
    "include",
    "packages",
    "host",
    "token",
    "profile",
    "warehouse_id",
    "master",
    "application_id",
    "execution_role",
    "s3_prefix",
    "cloud",
)

#: Renames between the decorator's Python-friendly names and the config keys the
#: backends use.
_ATTR_TO_CONFIG_KEY = {
    "application_id": "application-id",
    "execution_role": "execution-role",
    "s3_prefix": "s3-prefix",
}

#: Materializing more than this into a step's memory is usually a mistake at Spark
#: scale, so say something rather than let the task get OOM-killed later.
MATERIALIZE_WARN_BYTES = 1024**3


class SparkDecorator(StepDecorator):
    """Run Spark work as part of a step.

    Two shapes, chosen by whether a `job` is given:

    Session style, where the step body itself uses Spark::

        @spark(backend="databricks")
        @step
        def features(self):
            df = self.spark.read.table("main.retail.orders")
            self.count = df.count()

    Job style, where a separate function is handed a session and its result becomes an
    artifact. This is the only shape available on submit-style backends::

        @spark(backend="databricks", mode="job", job=myjob.run, volume="/Volumes/...")
        @step
        def features(self):
            print(len(self.spark_df))
    """

    name = "spark"
    defaults = {
        # what to run
        "job": None,
        "job_parameters": None,
        "session_attr": "spark",
        # where to run it
        "backend": None,
        "mode": None,
        "compute": None,
        "serverless": None,
        "cluster_id": None,
        "instance_pool_id": None,
        "num_workers": None,
        "node_type_id": None,
        "runtime_version": None,
        "photon": None,
        "dependencies": None,
        "libraries": None,
        "master": None,
        "cloud": None,
        # EMR Serverless
        "application_id": None,
        "execution_role": None,
        "s3_prefix": None,
        # workspace connection
        "host": None,
        "token": None,
        "profile": None,
        "warehouse_id": None,
        "volume": None,
        # code packaging
        "include": None,
        "packages": None,
        # output
        "output_artifact": "spark_df",
        "output_format": None,
        "output_table": None,
        "output_path": None,
        "write_mode": None,
        "partition_by": None,
        "save_output": True,
        # lifecycle
        "timeout": None,
        "crash_on_failure": True,
        # logs
        "show_stdout": True,
        "show_stderr": False,
        # cost attribution
        "tags": None,
        # configuration
        "config": "spark_config",
        "spark_parameters": None,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata = None
        self._run_id = None
        self._task_id = None
        self._user = None

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        output_format = self.attributes.get("output_format")
        if output_format is not None:
            validate_format(output_format)
        if self.attributes.get("job") is not None and not callable(
            self.attributes["job"]
        ):
            raise SparkConfigError(
                "@spark(job=...) must be a callable taking (spark, **job_parameters), "
                "got %r." % (self.attributes["job"],)
            )

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        # Captured here because the wrapper installed by task_decorate has no other way
        # to reach the metadata provider.
        self._metadata = metadata
        self._run_id = run_id
        self._task_id = task_id
        self._retry_count = retry_count

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return _spark_wrapper(self, step_func, flow, retry_count)

    # ------------------------------------------------------------------
    def record_metadata(self, entries):
        """Record job details as task metadata so a run stays inspectable later."""
        if self._metadata is None:
            return
        try:
            self._metadata.register_metadata(
                self._run_id,
                self.attributes.get("_step_name") or self._step_name,
                self._task_id,
                [
                    MetaDatum(
                        field=field,
                        value=value,
                        type=field,
                        tags=["attempt_id:%s" % self._retry_count],
                    )
                    for field, value in entries.items()
                    if value is not None
                ],
            )
        except Exception:
            # Metadata is a convenience. Never fail a working Spark job over it.
            pass


class PySparkDecorator(SparkDecorator):
    """Backwards-compatible alias for the original @pyspark decorator.

    Kept so existing flows keep running unchanged. The old boolean output attributes are
    translated to `output_format`, and the default backend stays EMR Serverless because
    that is what the original decorator targeted.
    """

    name = "pyspark"
    defaults = dict(
        SparkDecorator.defaults,
        output_pandas=None,
        output_pyarrow=None,
        user_timeout=None,
        spark_config=None,
    )

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        attrs = self.attributes
        if attrs.get("output_pandas") and attrs.get("output_format") is None:
            attrs["output_format"] = "pandas"
        elif attrs.get("output_pyarrow") and attrs.get("output_format") is None:
            attrs["output_format"] = "arrow"
        elif (
            attrs.get("output_pandas") is False
            and attrs.get("output_pyarrow") is False
            and attrs.get("output_format") is None
        ):
            attrs["output_format"] = "url"
        if attrs.get("user_timeout") is not None and attrs.get("timeout") is None:
            attrs["timeout"] = attrs["user_timeout"]
        if attrs.get("spark_config") is not None:
            attrs["config"] = attrs["spark_config"]
        if attrs.get("backend") is None:
            attrs["backend"] = "emr-serverless"
        super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )


# ----------------------------------------------------------------------
def _spark_wrapper(deco, step_func, flow, retry_count):
    attrs = deco.attributes

    @functools.wraps(step_func)
    def spark_step():
        from metaflow import current

        deco._step_name = current.step_name
        config = _resolve(flow, attrs)
        backend_name = config["backend"]
        section = backend_config(config, backend_name)

        job_func = attrs.get("job")
        output_format = attrs.get("output_format") or "pandas"
        validate_format(output_format)

        ctx = SparkJobContext(
            flow=flow,
            step_name=current.step_name,
            pathspec=current.pathspec,
            flow_name=current.flow_name,
            run_id=current.run_id,
            task_id=current.task_id,
            attempt=retry_count,
            user=_username(),
            config=section,
            tags={},
            inputs=_collect_inputs(flow, attrs.get("job_parameters")),
            job_func=job_func,
            timeout_minutes=config.get("timeout"),
            logger=log,
        )
        ctx.tags = build_tags(ctx, extra=attrs.get("tags"))

        backend = get_backend_class(backend_name)(section, ctx)

        if backend.kind == SESSION:
            _run_session(deco, backend, ctx, step_func, flow, attrs, output_format)
        else:
            _run_submit(deco, backend, ctx, step_func, flow, attrs, output_format)

    return spark_step


def _resolve(flow, attrs):
    """Build the effective config, then fold backend-specific attributes into it."""
    top_level = {
        key: attrs[key]
        for key in ("backend", "mode", "timeout")
        if attrs.get(key) is not None
    }
    if attrs.get("spark_parameters"):
        top_level["spark-parameters"] = attrs["spark_parameters"]

    config = resolve_config(flow, attrs.get("config"), top_level)

    backend_name = config["backend"]
    section_key = "databricks" if backend_name.startswith("databricks") else backend_name
    section = dict(config.get(section_key) or {})
    for attr in _BACKEND_ATTRS:
        value = attrs.get(attr)
        if value is not None:
            section[_ATTR_TO_CONFIG_KEY.get(attr, attr)] = value
    if attrs.get("mode") is not None:
        section["mode"] = attrs["mode"]
    config[section_key] = section
    # backend_config() reads the resolved backend's section under its own name.
    config[backend_name] = section
    return config


def _collect_inputs(flow, job_parameters):
    inputs = {}
    for name in job_parameters or []:
        if not hasattr(flow, name):
            raise SparkConfigError(
                "@spark(job_parameters=[...]) refers to '%s', which is not set on the "
                "flow at this point. Set self.%s in an earlier step."
                % (name, name)
            )
        inputs[name] = getattr(flow, name)
    return inputs


def _username():
    from metaflow.util import get_username

    try:
        return get_username()
    except Exception:
        return None


def _run_session(deco, backend, ctx, step_func, flow, attrs, output_format):
    from metaflow import current

    session_attr = attrs.get("session_attr") or "spark"
    job_func = ctx.job_func

    with backend.session(ctx) as session:
        # Exposed on `current` as well as on the flow, because `current.spark` is the
        # idiom people expect from other Metaflow decorators.
        current._update_env({"spark": session})
        try:
            if job_func is not None:
                df = job_func(session, **ctx.inputs)
                _set_output(deco, flow, attrs, from_spark_dataframe(df, output_format))
                step_func()
            else:
                # Set transiently: a SparkSession is not picklable, so it must be gone
                # before Metaflow persists the task's artifacts. The finally block runs
                # before task_post_step, which runs before persistence.
                setattr(flow, session_attr, session)
                try:
                    step_func()
                finally:
                    if getattr(flow, session_attr, None) is session:
                        delattr(flow, session_attr)
        finally:
            current._update_env({"spark": None})

    # Covers the step body's own toPandas() calls, which never pass through
    # from_spark_dataframe.
    for value in vars(flow).values():
        strip_spark_attrs(value)
    _cleanup_output(flow, attrs)


def _run_submit(deco, backend, ctx, step_func, flow, attrs, output_format):
    handle, status = backend.run(
        ctx,
        show_stdout=attrs.get("show_stdout", True),
        show_stderr=attrs.get("show_stderr", False),
        crash_on_failure=attrs.get("crash_on_failure", True),
    )

    deco.record_metadata(
        {
            "spark-backend": backend.name,
            "spark-job-id": handle.job_id,
            "spark-job-url": handle.ui_url,
            "spark-ui-url": handle.spark_ui_url,
            "spark-job-handle": json.dumps(handle.to_dict()),
        }
    )
    if handle.ui_url:
        ctx.log("job details: %s" % handle.ui_url)
    if handle.spark_ui_url:
        ctx.log("Spark UI: %s" % handle.spark_ui_url)

    result = None
    if attrs.get("output_artifact"):
        if status.ok:
            reader = getattr(backend, "read_output", None)
            result = (
                reader(handle, output_format)
                if reader
                else backend.result(handle)
            )
        _set_output(deco, flow, attrs, result)

    try:
        step_func()
    finally:
        cleanup = getattr(backend, "cleanup", None)
        if cleanup:
            try:
                cleanup(handle, ctx)
            except Exception:
                pass

    _cleanup_output(flow, attrs)


def _set_output(deco, flow, attrs, value):
    artifact = attrs.get("output_artifact")
    if not artifact:
        return
    _warn_if_large(value, artifact)
    setattr(flow, artifact, value)


def _cleanup_output(flow, attrs):
    artifact = attrs.get("output_artifact")
    if artifact and not attrs.get("save_output", True) and hasattr(flow, artifact):
        delattr(flow, artifact)


def _warn_if_large(value, artifact):
    """Nudge toward a reference instead of a copy when a result is big.

    Collecting a Spark result into the task process is convenient and is the historical
    default, but at Spark scale it is the wrong shape. A warning is more useful than
    silently changing the default under people.
    """
    size = None
    try:
        if hasattr(value, "nbytes"):
            size = value.nbytes
        elif hasattr(value, "memory_usage"):
            size = int(value.memory_usage(deep=True).sum())
        elif hasattr(value, "estimated_size"):
            size = value.estimated_size()
    except Exception:
        return
    if size and size > MATERIALIZE_WARN_BYTES:
        warnings.warn(
            "@spark materialized %.1fGB into 'self.%s'. Consider "
            "output_format='table' or 'url' to pass a reference to the next step "
            "instead of copying the data." % (size / 1024**3, artifact),
            stacklevel=2,
        )

"""SQL warehouse access: run a statement without a Spark session or a cluster.

Most people who say "I need Spark" need to read some data, and a SQL warehouse answers
that without paying for Spark compute at all. `query()` is a plain function rather than a
decorator, deliberately: the point of this capability is specifically *not* needing a
`@spark` step, a compute-shape config, or a live session.

::

    from metaflow_extensions.spark.plugins.warehouse import query

    @step
    def analyze(self):
        self.daily = query(
            "SELECT order_date, SUM(amount) AS revenue FROM main.retail.orders "
            "WHERE order_date >= :since GROUP BY order_date",
            params={"since": self.since},
            output_format="polars",
        )

`params` are bound as named, typed parameters through the Statement Execution API's
``:name`` markers, never concatenated into the statement text. This is what closes the
injection surface that ``self.spark.sql("... {0} ...".format(...))`` leaves open.

**Target resolution**, per ``plans/scope.md`` L6: an explicit ``warehouse_id=`` wins, else
a live session on the calling step (``self.spark`` from an enclosing ``@spark`` step, if
any) is reused rather than opening a new warehouse round trip, else the configured
default (``warehouse_id`` in the ``databricks`` config section, or
``METAFLOW_DATABRICKS_WAREHOUSE_ID`` / ``DATABRICKS_WAREHOUSE_ID``).

**Limits worth documenting rather than discovering.** Unlike a pinned
``UnityCatalogTable``, an arbitrary statement is not reproducible: the tables underneath
it can move between runs, and this module does not rewrite user SQL to imply otherwise.
And this is a different mechanism from ``catalog/unity.py``'s credential vending: a
warehouse statement goes through the query engine, so views, row filters, and column
masks apply here, where vending bypasses the engine entirely.

No structured task metadata is recorded for a `query()` call (no `spark-statement-id`
the way a `@spark` job gets a `spark-job-id`). Registering it needs the metadata
provider object Metaflow only hands to a `StepDecorator`'s lifecycle hooks, and `query()`
is deliberately a plain function with no such hook. The statement id and warehouse URL
are still visible in the task's log output.

Cost attribution is via `query_tags` (public preview, needs `databricks-sdk>=0.86`),
which lands in `system.query.history`, not `system.billing.usage`, so it is a different
join key from `cost.py::DATABRICKS_USAGE_QUERY` rather than an extension of it.
"""

from .config import backend_config, require, resolve_config
from .context import SparkJobContext, log
from .cost import build_tags
from .exceptions import SparkConfigError

#: `query()` returns a statement result, which has no live DataFrame and no storage
#: location, so the "spark", "table", and "url" formats in output.py's full vocabulary
#: are not meaningful here. Keep the subset explicit rather than letting an
#: unsupported choice fail deep inside a backend.
QUERY_FORMATS = ("pandas", "arrow", "polars", "none")

WAREHOUSE_HINT = (
    "Set it with query(warehouse_id=...), the 'warehouse_id' key of the 'databricks' "
    "config section, or METAFLOW_DATABRICKS_WAREHOUSE_ID / DATABRICKS_WAREHOUSE_ID."
)


def query(
    statement,
    params=None,
    output_format="pandas",
    warehouse_id=None,
    flow=None,
    config="spark_config",
    catalog=None,
    schema=None,
    row_limit=None,
    byte_limit=None,
    timeout=None,
    tags=None,
    crash_on_failure=True,
):
    """Run one SQL statement on a Databricks SQL warehouse and return its result.

    `flow` is optional and only needed to pick up a flow-level config artifact (by
    default `self.spark_config`) the way `@spark(config=...)` does; pass `flow=self`
    from inside a step to get that layer of the precedence chain. Without it, the
    configured default still resolves from `METAFLOW_DATABRICKS_WAREHOUSE_ID` /
    `DATABRICKS_WAREHOUSE_ID` and explicit arguments, just not from a flow artifact.
    """
    if output_format not in QUERY_FORMATS:
        raise SparkConfigError(
            "query(output_format=%r) is not meaningful for a statement result, since "
            "there is no Spark DataFrame or storage location to reference. Choose one "
            "of: %s." % (output_format, ", ".join(QUERY_FORMATS))
        )

    overrides = {"backend": "databricks"}
    if timeout is not None:
        overrides["timeout"] = timeout
    resolved = resolve_config(flow, config, overrides)
    section = dict(backend_config(resolved, "databricks"))

    session = None
    if warehouse_id:
        section["warehouse_id"] = warehouse_id
    else:
        session = _live_session()
        if session is None:
            require(section, "warehouse_id", "databricks-sql", WAREHOUSE_HINT)

    if session is not None:
        return _query_via_session(session, statement, params, output_format)

    section["statement"] = statement
    section["parameters"] = params or {}
    for key, value in (
        ("catalog", catalog),
        ("schema", schema),
        ("row_limit", row_limit),
        ("byte_limit", byte_limit),
    ):
        if value is not None:
            section[key] = value

    ctx = _build_context(flow, section, resolved, tags)

    from .backends.databricks.sql import DatabricksSqlBackend

    backend = DatabricksSqlBackend(section, ctx)
    handle, _ = backend.run(
        ctx, show_stdout=False, show_stderr=True, crash_on_failure=crash_on_failure
    )
    return backend.read_output(handle, output_format)


def _live_session():
    """The session an enclosing `@spark` step already established, if any.

    Reusing it rather than opening a warehouse connection is correct because that
    compute is already paid for and running; see `decorator.py::_run_session`, which is
    what sets `current.spark`.
    """
    from metaflow import current

    return getattr(current, "spark", None)


def _query_via_session(session, statement, params, output_format):
    """Run the statement through a live Spark session instead of a warehouse.

    Uses the same `:name` marker syntax as the warehouse path: PySpark's
    `SparkSession.sql(sqlQuery, args=...)` accepts named parameters with identical
    syntax, so the statement text does not need to change between the two paths.
    """
    from .output import from_spark_dataframe

    df = session.sql(statement, args=params or None)
    return from_spark_dataframe(df, output_format)


def _build_context(flow, section, resolved, tags):
    from metaflow import current

    ctx = SparkJobContext(
        flow=flow,
        step_name=current.step_name,
        pathspec=current.pathspec,
        flow_name=current.flow_name,
        run_id=current.run_id,
        task_id=current.task_id,
        attempt=getattr(current, "retry_count", 0) or 0,
        user=_username(),
        config=section,
        tags={},
        timeout_minutes=resolved.get("timeout"),
        logger=log,
    )
    ctx.tags = build_tags(ctx, extra=tags)
    return ctx


def _username():
    from metaflow.util import get_username

    try:
        return get_username()
    except Exception:
        return None

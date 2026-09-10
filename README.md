# metaflow-spark

Spark for Metaflow steps, with first-class Databricks support.

```python
from metaflow import FlowSpec, spark, step

class MyFlow(FlowSpec):

    @spark(backend="databricks")
    @step
    def features(self):
        df = self.spark.read.table("main.retail.orders")
        self.daily = df.groupBy("order_date").count().toPandas()
        self.next(self.end)
```

That step runs unchanged against a local Spark, against Databricks through Spark Connect, or
submitted to a Databricks Job, depending on configuration that lives outside the step body.

> Status: experimental. The `@pyspark` decorator this repo started as is still here and still
> works, see [backwards compatibility](#backwards-compatibility).

## Install

```bash
pip install -e '.[databricks]'      # Databricks Connect, Jobs, Unity Catalog
pip install -e '.[catalog]'         # cluster-free reads via credential vending
pip install -e '.[local]'           # local pyspark, needs a JVM
pip install -e '.[emr]'             # EMR Serverless
```

`databricks-connect` bundles its own `pyspark`. Do not install `[local]` and `[databricks]` into
the same environment; the extension detects that clash and says so instead of letting the import
error point nowhere useful.

Authentication is delegated entirely to `databricks-sdk`. PATs, CLI profiles, OAuth service
principals, and Azure MSI all work the way they already do:

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com DATABRICKS_TOKEN=dapi...
# or
export DATABRICKS_CONFIG_PROFILE=my-workspace
```

## Backends

| Backend | `backend=` | How it runs |
| --- | --- | --- |
| Local Spark | `local` | a `SparkSession` in the task process |
| Databricks Connect | `databricks` (default mode) | session against Databricks compute, driver logic stays in the task |
| Databricks Jobs | `databricks` with `mode="job"` | code packaged, staged on a UC Volume, submitted through the Jobs API |
| Existing Databricks job | `databricks` with `mode="existing"`, or `@databricks_job` | `run-now` against a job the workspace already has |
| EMR Serverless | `emr-serverless` | packaged, staged on S3, submitted as a job run |

Connect mode is the one to reach for first: there is no code package and no pickle round trip;
the step's own process holds the driver logic. Job mode is what you need for a pinned Databricks
Runtime, Photon, cluster-scoped libraries, or JVM UDFs.

Compute is described the same way whichever mode you use: `serverless=True`, `cluster_id=...`,
`instance_pool_id=...`, or `num_workers=` plus `node_type_id=` for a fresh job cluster, with
`photon=True` and `runtime_version=` on top. Conflicting shapes are rejected at submit time rather
than silently ignored.

## Two shapes for a step

**Session style**, where the step body uses Spark directly and `self.spark` is a live session. Best
for iterating, and the only shape where local variables in the step are usable inside the Spark code.

**Job style**, where a separate function is handed a session and its return value becomes an artifact.
Required on submit-style backends, and the shape that lets Spark code live in its own importable,
testable module:

```python
@spark(backend="databricks", mode="job", job=jobs.etl.summarize, job_parameters=["cutoff"])
@step
def crunch(self):
    print(self.spark_df.head())
```

The job function must not import metaflow; it runs on the cluster. Its module ships with the rest
of its package: a job can span more than one file, and `include=[...]` adds anything else it needs.

## Output formats

`output_format` decides what lands in the artifact: `pandas`, `arrow`, `polars`, `table`, `url`,
`spark`, or `none`. The materializing formats copy the result into the task, which is convenient and
wrong past a certain size. `table` and `url` pass a reference to the next step instead. A result
over 1GB gets a warning pointing at those.

## Unity Catalog tables as artifacts

```python
self.orders = UnityCatalogTable("main.retail.orders")   # pins the current Delta version
```

The artifact is a reference, not a copy. Assignment records the table's current Delta version. A
re-run reads the same bytes even if the table has moved on, and the Metaflow datastore stays small
while governed data stays governed in UC.

Reads go two ways. `to_spark(session)` reads through Spark, honouring the pin, which is right for
large tables and joins. `to_arrow()`, `to_pandas()`, `to_polars()`, and `to_duckdb()` ask Unity
Catalog to vend temporary, table-scoped credentials and read the Delta files directly, with no cluster
involved at all. Governance is intact either way: UC issues the credentials and enforces the
grants. Vending needs `EXTERNAL USE SCHEMA` in addition to `SELECT`, and says so when it is missing.

## SQL warehouses: a statement, no cluster, no session

```python
from metaflow_extensions.spark.plugins.warehouse import query

self.daily = query(
    "SELECT order_date, SUM(amount) AS revenue FROM main.retail.orders "
    "WHERE order_date >= :since GROUP BY order_date",
    params={"since": self.since},
    output_format="polars",
)
```

Unlike `self.spark.sql(...)`, this needs no `@spark` step, no compute-shape configuration, and no
live session: `query()` submits the statement to a Databricks SQL warehouse and polls it with the
same wait/cancel/error-taxonomy contract every other backend uses. `params` bind as named, typed
parameters through the statement's `:name` markers, never concatenated into the SQL text. Unlike
credential vending, a warehouse statement goes through the query engine, so views, row filters, and
column masks apply.

Target resolution: an explicit `warehouse_id=` wins, else a live session on the calling step
(`self.spark`, if the step is also decorated with `@spark`) is reused rather than opening a new
warehouse connection, else the configured default (`warehouse_id` in the `databricks` config section,
or `METAFLOW_DATABRICKS_WAREHOUSE_ID` / `DATABRICKS_WAREHOUSE_ID`). `output_format` is `pandas`,
`arrow`, `polars`, or `none`; `table` and `url` are not meaningful for a statement result and are
rejected up front. See [`demos/03_no_cluster`](demos/03_no_cluster) for this next to credential
vending and a Spark session, measured side by side.

## Cost attribution

Every submitted job carries `metaflow_flow`, `metaflow_run_id`, `metaflow_step`, `metaflow_task_id`,
`metaflow_pathspec`, `metaflow_attempt`, and `metaflow_user` as provider tags, plus anything in
`@spark(tags=...)`. On Databricks those land in `system.billing.usage`, which turns a monthly total
into a per-flow, per-run, per-step breakdown. See
[`demos/06_cost_attribution`](demos/06_cost_attribution).

`query()` statements carry the same tags, but as `query_tags` on the statement rather than
`custom_tags` on compute, since a warehouse is shared, long-lived compute rather than a job's own
cluster. Those land in `system.query.history`, a different join key from `system.billing.usage`, so
attributing warehouse spend needs its own query rather than reusing the job-run one. `query_tags`
needs `databricks-sdk>=0.86`; on an older SDK, `query()` still runs, just without the tags.

## Failures, cancellation, and metadata

Job state comes back classified: `SparkJobFailed` carries the provider's error class and the driver
logs, `SparkJobTimeout` and `SparkJobCancelled` are distinct from it, and a control-plane outage
raises `SparkControlPlaneError` without cancelling a job that is probably fine. Interrupting a run,
or hitting a `@timeout`, cancels the remote job instead of leaving it running and billing.

Each step records `spark-backend`, `spark-job-id`, `spark-job-url`, `spark-ui-url`, and a serialized
job handle as task metadata. A finished run stays inspectable:

```python
Task("MyFlow/42/features/1").metadata_dict["spark-job-url"]
```

## Configuration

Lowest to highest precedence:

1. backend defaults
2. `METAFLOW_SPARK_*` and standard `DATABRICKS_*` environment variables, or the Metaflow config
3. a flow-level config artifact, by default `self.spark_config`, as a dict, JSON string, or
   `IncludeFile`
4. explicit decorator attributes

So the same flow file moves between environments without editing the flow, which is what
[`demos/01_hello_spark`](demos/01_hello_spark) demonstrates by running one file three ways.

## Demos

Five demos are built. See [`demos/`](demos) for the set,
[`plans/databricks-integration.md`](plans/databricks-integration.md) for the customer questions and
the architecture chosen in response, and [`plans/scope.md`](plans/scope.md) for the phased delivery
plan.

## Backwards compatibility

`@pyspark` is now an alias for `@spark` that defaults to EMR Serverless and translates the old
`output_pandas` and `output_pyarrow` booleans. Flows written against the original decorator keep
running. `example/sparkflow.py` is one of those, kept as the compatibility example.

## Tests

```bash
python -m pytest tests -q
```

No cloud account needed. Backends are faked, and four of the tests run real Metaflow flows in a
subprocess to cover session detachment before artifact persistence, artifact ordering, metadata
registration, and both failure modes.

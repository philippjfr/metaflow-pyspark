# Choose a Backend

`@spark(backend=...)` picks which compute runs the step. All backends share the same
decorator, the same output formats, the same failure taxonomy, and the same cost tags;
only how the Spark work actually runs differs.

| Backend | `backend=` | How it runs |
| --- | --- | --- |
| Local Spark | `"local"` | a `SparkSession` in the task process |
| Databricks Connect | `"databricks"` (default mode) | session against Databricks compute, driver logic stays in the task |
| Databricks Jobs | `"databricks"` with `mode="job"` | code packaged, staged on a UC Volume, submitted through the Jobs API |
| Existing Databricks job | `"databricks"` with `mode="existing"`, or `@databricks_job` | `run-now` against a job the workspace already has |
| EMR Serverless | `"emr-serverless"` | packaged, staged on S3, submitted as a job run |

## Databricks Connect: reach for this first

```python
@spark(backend="databricks")
@step
def features(self):
    df = self.spark.read.table("main.retail.orders")
```

There is no code package and no pickle round trip: the step's own process holds the
driver logic, and every read goes through the cluster's Unity Catalog credentials with
nothing new to configure. Use `serverless=True` for the fastest path to compute, or
`cluster_id=...`/`instance_pool_id=...` to target something already running.

## Databricks Jobs: when Connect is not enough

```python
@spark(backend="databricks", mode="job", job=jobs.etl.summarize,
       runtime_version="15.4.x-scala2.12", photon=True, num_workers=4)
@step
def crunch(self):
    print(self.spark_df.head())
```

Reach for job mode when you need a pinned Databricks Runtime, Photon, cluster-scoped
libraries, or JVM UDFs — anything that has to run *on* the cluster rather than through a
Connect session. This is also the only mode available on submit-style backends, so the
step body cannot use `self.spark` directly; see
[Session or Job Style](session-vs-job.md).

## An existing Databricks Job or notebook

```python
@databricks_job(job_name="raw-orders-ingest", parameters_from=["run_date"])
@step
def ingest(self):
    print(self.databricks_result)

@databricks_notebook(notebook_path="/Repos/de/validate", parameters_from=["run_date"])
@step
def validate(self):
    print(self.notebook_result)
```

Neither decorator changes anything in the workspace. `@databricks_job` resolves
`job_name` at submit time — so a job an Asset Bundle redeploys with a new id keeps
working — waits for it, and returns each task's exit value keyed by task key.
`@databricks_notebook` runs an existing notebook and returns the value passed to
`dbutils.notebook.exit()`. Use these to orchestrate pipelines you are not ready to
rewrite, and to migrate one job at a time later.

## Compute shapes

Whichever mode you use, compute is described the same way:

```python
@spark(backend="databricks", serverless=True)                       # serverless
@spark(backend="databricks", cluster_id="0801-...")                  # existing cluster
@spark(backend="databricks", instance_pool_id="0801-...-pool")       # instance pool
@spark(backend="databricks", num_workers=4, node_type_id="i3.xlarge") # fresh job cluster
```

Add `photon=True` and `runtime_version=...` on top of any shape. Conflicting shapes —
for example `serverless=True` together with `num_workers=`  — are rejected at submit
time rather than silently ignored, since a resolved-but-wrong shape is how someone gets
a surprising bill.

## Backwards compatibility

`@pyspark` is an alias for `@spark` defaulting to EMR Serverless. See
[Migrate from @pyspark](migrate-from-pyspark.md).

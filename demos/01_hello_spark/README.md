# Demo 1: the same flow, three places to run it

The opening question on most calls is some version of "is this real Spark, or a lookalike". The answer
is a flow whose step body does not change between a laptop, Databricks serverless, and a pinned
Databricks Runtime with Photon.

## Local, no account

```bash
pip install -e '../..[local]'      # needs a JVM
python hello_spark.py run
```

`self.spark` is a local `SparkSession`. Useful for CI and for iterating on transformation logic
without paying for anything.

## Databricks serverless, through Spark Connect

```bash
pip install -e '../..[databricks]'
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com DATABRICKS_TOKEN=dapi...
METAFLOW_SPARK_BACKEND=databricks python hello_spark.py run
```

Same file, same body. `self.spark` is now a session against Databricks compute: the step's Python runs
wherever Metaflow put it, and only the query plan crosses the wire. No cluster to configure, and
`session.version` prints the Databricks Runtime's Spark version, which settles the "real Spark"
question without a slide.

`databricks-connect` bundles its own `pyspark` and the two conflict, so use a separate environment
from the local backend. The extension detects that case and says so rather than letting the import
error point nowhere.

## Submitted as a job, on a pinned runtime

```bash
METAFLOW_SPARK_BACKEND=databricks METAFLOW_SPARK_MODE=job \
METAFLOW_DATABRICKS_RUNTIME_VERSION=15.4.x-scala2.12 \
METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging \
    python hello_spark.py run
```

Connect mode runs the driver logic in the Metaflow task, which is what you want most of the time.
When you need a specific DBR, Photon, cluster-scoped libraries, or JVM UDFs, the work has to run on
the cluster instead. `photon_job.py` shows that as decorator attributes rather than environment
variables:

```bash
METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging python photon_job.py run
```

In job mode the Spark code is a function that takes a session and returns a DataFrame, which the
extension packages, stages on a UC Volume, and submits through the Jobs API. `jobs/etl.py` imports
from `jobs/helpers.py` on purpose: the whole `jobs/` package ships, so a job is allowed to be more
than one file. The job code must not import metaflow, because it runs on the cluster.

## Files

| File | What it shows |
| --- | --- |
| `hello_spark.py` | one `@spark` step, three backends, chosen by environment variable |
| `photon_job.py` | `mode="job"` with a pinned runtime, Photon, and cost-center tags |
| `jobs/etl.py`, `jobs/helpers.py` | multi-file job code, packaged and shipped together |

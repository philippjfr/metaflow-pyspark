# Quickstart

Run one Spark step three ways — on a laptop, against Databricks serverless through
Spark Connect, and submitted as a Databricks Job — without changing the step body. By
the end of this page you will have a flow that answers "is this real Spark, or a
lookalike" by running rather than by arguing.

## Install

```bash
pip install -e '.[local]'          # local Spark, needs a JVM
# or
pip install -e '.[databricks]'     # Databricks Connect and Jobs
```

`databricks-connect` bundles its own `pyspark`, so keep the `local` and `databricks`
extras in separate environments.

## Write the flow

```python
from metaflow import FlowSpec, Parameter, spark, step


class HelloSparkFlow(FlowSpec):
    rows = Parameter("rows", default=1_000_000)

    @step
    def start(self):
        self.next(self.crunch)

    @spark
    @step
    def crunch(self):
        session = self.spark
        print("Spark %s" % session.version)

        df = session.range(self.rows).selectExpr(
            "id", "id %% 7 AS bucket", "rand(42) AS value"
        )
        summary = (
            df.groupBy("bucket")
            .agg({"value": "avg", "id": "count"})
            .orderBy("bucket")
        )
        self.summary = summary.toPandas()
        self.spark_version = session.version
        self.next(self.end)

    @step
    def end(self):
        print(self.summary.to_string(index=False))
        print("\nran on Spark %s" % self.spark_version)


if __name__ == "__main__":
    HelloSparkFlow()
```

`self.spark` is a live `SparkSession`. With no other configuration `@spark` defaults to
the `local` backend, so this runs with no account and no credentials.

## Run it locally

```bash
python hello_spark.py run
```

## Point the same file at Databricks

Nothing in the flow changes. Configuration lives in the environment, in Metaflow
config, in a flow-level config artifact, or in explicit decorator attributes — see
[Configure Compute and Credentials](how-to/configuration.md) for the full precedence
order.

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
METAFLOW_SPARK_BACKEND=databricks python hello_spark.py run
```

`self.spark` is now a session against Databricks compute: the step's Python runs
wherever Metaflow put it, and only the query plan crosses the wire.

## Next steps

- [Choose a Backend](how-to/choose-a-backend.md) for the full backend list and when
  each one applies.
- [Hello Spark: One Flow, Three Backends](tutorials/hello-spark.md) for the version of
  this flow that also submits as a job on a pinned runtime with Photon.

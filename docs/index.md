# metaflow-spark

**Spark for Metaflow steps, with first-class Databricks support.**

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

That step runs unchanged against a local Spark session, against Databricks through
Spark Connect, or submitted to a Databricks Job, depending on configuration that lives
outside the step body.

!!! tip "Quick start"
    New to `@spark`? **[Try the quickstart →](quickstart.md)**

!!! warning "Status: experimental"
    The `@pyspark` decorator this repo started as is still here and still works. See
    [Migrate from @pyspark](how-to/migrate-from-pyspark.md).

## Why metaflow-spark

- **One decorator, several backends** — local Spark, Databricks Connect, Databricks
  Jobs, an existing Databricks Job, or EMR Serverless, chosen by config rather than by
  rewriting the step.
- **Two shapes for a step** — a live session for iterating, or a plain job function for
  submit-style backends where the driver has to run on the cluster.
- **Governed data as an artifact** — `UnityCatalogTable` pins a table's Delta version at
  assignment, so a re-run reads the same bytes even after the table has moved on.
- **Cost and failure attached to the run** — every submitted job carries the Metaflow
  pathspec as a provider tag, and failures come back classified instead of as
  "check the UI".

## Install

```bash
pip install -e '.[databricks]'      # Databricks Connect, Jobs, Unity Catalog
pip install -e '.[catalog]'         # cluster-free reads via credential vending
pip install -e '.[local]'           # local pyspark, needs a JVM
pip install -e '.[emr]'             # EMR Serverless
```

Metaflow is not a hard dependency, so the extension installs next to `outerbounds` without
pulling PyPI `metaflow` over `ob-metaflow`. On open-source Metaflow, add the `metaflow` extra:
`pip install -e '.[metaflow,databricks]'`.

`databricks-connect` bundles its own `pyspark`. Do not install `[local]` and
`[databricks]` into the same environment; the extension detects that clash and says so
instead of letting the import error point nowhere useful.

## How-to guides

- [Choose a Backend](how-to/choose-a-backend.md)
- [Session or Job Style](how-to/session-vs-job.md)
- [Configure Compute and Credentials](how-to/configuration.md)
- [Choose an Output Format](how-to/output-formats.md)
- [Read & Write Unity Catalog Tables](how-to/unity-catalog.md)
- [Attribute Cost](how-to/cost-attribution.md)
- [Handle Failures & Cancellation](how-to/failures-and-cancellation.md)
- [Migrate from @pyspark](how-to/migrate-from-pyspark.md)

## Tutorials

- [Tutorials gallery](tutorials/index.md) — the demos in [`demos/`](https://github.com/outerbounds/metaflow-pyspark/tree/main/demos),
  walked through end to end.

## Reference

- [API reference](reference/index.md)

# Tutorials

Runnable walkthroughs, one per chapter in [`demos/`](https://github.com/outerbounds/metaflow-pyspark/tree/main/demos). Chapter 1 shows that a step does not care where Spark runs. Chapters 2 to 4 then hold one table (`main.retail.orders`) and one question (daily orders and revenue) fixed and change only how the data is reached, so differences in the output are differences between the access paths. Chapters 5 to 7 reuse the same table for governance, existing pipelines, and cost.

| Tutorial | Changes | Needs a Databricks account |
| --- | --- | --- |
| [The Shared Table](seed.md) | creates the table every chapter reads | yes |
| [Hello Spark: One Flow, Three Backends](hello-spark.md) | where Spark runs | no, runs locally too |
| [Reading: One Statement, Four Engines](reading.md) | who reads: Connect, a job, credential vending, a warehouse | yes |
| [Results: Copy or Reference](results.md) | what a Spark step hands on | yes, plus a Volume |
| [Writing: One Summary, Four Writers](writing.md) | who writes, and what lineage records | yes, plus a Volume |
| [Governance and Reproducibility](reproducibility.md) | when the table is read | yes |
| [Orchestrating Existing Jobs](existing-jobs.md) | who owns the pipeline | yes |
| [Cost Attribution](cost-attribution.md) | who pays | yes, plus system tables |

## The four paths

| | Connect session | Job on Databricks | Credential vending | SQL warehouse |
| --- | --- | --- | --- | --- |
| In a step | `@spark`, then `self.spark` | `@spark(mode="job", job=fn)` | `UnityCatalogTable(...).to_duckdb()` and friends | `query(...)` |
| Where the work runs | Databricks serverless, driver in the task | a Databricks job | the Metaflow task | a warehouse |
| Paid before it runs | session start | job compute start | nothing | warehouse wake-up |
| DBUs | yes | yes | none | SQL DBUs |
| Views, row filters, column masks | yes | yes | no | yes |
| Extra grant | | `WRITE VOLUME` for staging | `EXTERNAL USE SCHEMA` | `CAN USE` on the warehouse |
| Result comes back as | Arrow over Connect | staged Parquet, or a pinned table reference | Arrow from the Delta files | Arrow through external links |
| Writes | `saveAsTable`, SQL | `output_table=` | `UnityCatalogTable.write()`, external tables only | `CREATE TABLE AS`, `INSERT`, `MERGE` |
| Unity Catalog lineage | yes | yes | no | yes |
| Cost attribution | session tag in query history | `custom_tags` in `system.billing.usage` | nothing to attribute | `query_tags` in `system.query.history` |

## Setup

```bash
pip install -e '.[databricks,catalog]' duckdb
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

Any authentication the Databricks SDK understands works, including `~/.databrickscfg` profiles and OAuth service principals:

```bash
export DATABRICKS_CONFIG_PROFILE=my-workspace
```

Hello Spark needs none of this. Everything else needs the table [The Shared Table](seed.md) creates, a warehouse for the `query()` paths, and a Unity Catalog Volume for the job paths:

```bash
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
```

Each demo directory has its own README with the exact commands. Run flows from inside their directory, since they import `_env.py`, `_orders.py`, and job modules sitting next to them.

## Which tutorial answers which objection

**"Is this actually Spark."** [Hello Spark](hello-spark.md), in the three-backend form, live.

**"Spark is expensive for what we do."** [Reading](reading.md). Same table, same grants, same answer from four engines, and the cost of each is a measurement.

**"Does this respect our governance."** [Writing](writing.md) and [reproducibility](reproducibility.md). Every path gets its access from Unity Catalog, every output is tagged with the task that wrote it, and Metaflow pins versions rather than copying data out, which [results](results.md) measures.

**"We already have Databricks, why add anything."** [Existing jobs](existing-jobs.md), then [cost attribution](cost-attribution.md). Metaflow orchestrates what exists without touching it, then shows where the money went.

Not built yet: debugging and observability needs the Spark card, an MLflow bridge, a notebook-triggered flow, a production-project reference, and an end-to-end composite.

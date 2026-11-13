# Demos

One table, one question, and every way the extension offers to get at the data and write results back. Chapter 1 shows that a step does not care where Spark runs. Chapters 2 to 4 then hold the table (`main.retail.orders`) and the question (daily orders and revenue, the same `DAILY_SQL` throughout) fixed, and change only the access path, so differences in the output are differences between the paths. Chapters 5 to 7 reuse the same table and statement for governance, existing pipelines, and cost.

| Chapter | Changes | Shows |
| --- | --- | --- |
| [`00_seed`](00_seed) | | creates the table, a view over it, and an output table, with warehouse statements |
| [`01_hello_spark`](01_hello_spark) | where Spark runs | one step body on local Spark, Connect, and a submitted job; no account needed |
| [`02_reading`](02_reading) | who reads | the same statement through Connect, a job, credential vending, and a warehouse, checked equal and timed |
| [`03_results`](03_results) | what comes back | the same job returned as a DataFrame, all rows, a URL, and a pinned table reference, with artifact sizes |
| [`04_writing`](04_writing) | who writes | the same summary written by Connect, a job, a warehouse, and vended credentials, each pinned and tagged |
| [`05_reproducibility`](05_reproducibility) | when it is read | pin, mutate, replay: a re-run reads the snapshot the original saw |
| [`06_existing_jobs`](06_existing_jobs) | who owns the pipeline | orchestrate existing jobs and notebooks unchanged, then port one |
| [`07_cost_attribution`](07_cost_attribution) | who pays | compute shapes timed, and billing grouped by Metaflow run and step |

## The four paths

Every data chapter fills in part of this table:

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
pip install -e '..[databricks,catalog]' duckdb
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

Any authentication the Databricks SDK understands works, including `~/.databrickscfg` profiles and OAuth service principals, because the extension delegates the whole resolution chain to the SDK:

```bash
export DATABRICKS_CONFIG_PROFILE=my-workspace
```

Chapter 1 needs none of this. Everything else needs the table chapter 0 creates, a warehouse for the `query()` paths, and a Unity Catalog Volume for the job paths:

```bash
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
(cd 00_seed && python seed.py run)
```

Run each flow from inside its directory. Every directory links `_env.py` and [`_orders.py`](_orders.py), the shared table names, statement, and job function, so `import _orders` works there and Metaflow packages it with the flow. A path that is not configured, such as a job without a Volume, is reported by the chapters that compare paths rather than failing the run.

## Running on Outerbounds

Install the extension next to `outerbounds` on the machine you launch from; Metaflow ships it to the task pods in the code package. Then pick a virtual environment and send the steps to Kubernetes:

```bash
python four_ways.py --environment=fast-bakery run --with kubernetes
python four_ways.py --environment=fast-bakery argo-workflows create
```

Every step carries a `@step_env(...)` from [`_env.py`](_env.py), which does nothing under the default local environment and, under `fast-bakery`, gives the step:

- `@anaconda` from Anaconda's main channel, or `@pypi` for steps that need `databricks-connect` (PyPI only) or `deltalake` (conda-forge only, and incompatible with Anaconda's Python);
- `@secrets(sources=["outerbounds.databricks"])` for `DATABRICKS_HOST` and `DATABRICKS_TOKEN`;
- `@environment` carrying the settings above (`METAFLOW_DATABRICKS_VOLUME`, `DATABRICKS_WAREHOUSE_ID`, `METAFLOW_SPARK_BACKEND`, and the rest of `FORWARDED_SETTINGS`) from the launching shell into the pods. For a deployment they are captured at `create` time.

The pods need outbound HTTPS to the Databricks workspace and, for credential vending, to the table's cloud storage.

## Which chapter answers which objection

**"Is this actually Spark."** Chapter 1, in the three-backend form, live.

**"Spark is expensive for what we do."** Chapter 2. Same table, same grants, same answer from four engines, and the cost of each is a measurement.

**"Does this respect our governance."** Chapters 4 and 5. Every path gets its access from Unity Catalog, every output is tagged with the task that wrote it, and Metaflow adds version pinning on top rather than copying data out, which chapter 3 measures.

**"We already have Databricks, why add anything."** Chapter 6, then 7. Metaflow orchestrates what exists without touching it, and then tells them where their money went, which their platform cannot.

## Not built yet

Debugging and observability needs the Spark card, `mlflow_together` the MLflow bridge, `from_a_notebook` both, `production_project` needs `export-bundle`, an end-to-end reference composes all of them, and `environments` needs `@pypi` parity on the Spark side. [`../plans/scope.md`](../plans/scope.md) lays out the capability each depends on.

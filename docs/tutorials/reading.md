# Reading: One Statement, Four Engines

```bash
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging   # for the job branch
python four_ways.py run --warehouse-id <id>
python four_ways.py run --warehouse-id <id> --table main.retail.orders_public
```

Reads the table [chapter 0](seed.md) creates. The flow pins it once in `start`, then forks into four branches that run the same `DAILY_SQL` against that version. `compare` checks that all four answers match and prints what each one cost.

These four get conflated on calls, and the difference decides where the money goes.

**Connect** is a Spark client. `self.spark` is a real session whose driver logic runs in the Metaflow task and whose execution happens on Databricks serverless. You get Databricks' Spark, Photon, Unity Catalog enforcement inside the engine, and the whole DataFrame API, and you pay for the session from the moment it opens.

**Job** runs the same statement as a function, `daily_revenue(spark, table_name, version)`, packaged with its module, staged on a Volume, and submitted through the Jobs API. The result comes back as Parquet on that Volume rather than over the wire. This is the path for a pinned runtime, cluster libraries, or work that should not depend on the task staying alive.

**Vending** is not Spark at all. Unity Catalog checks the grant and issues a short-lived credential scoped to the table's storage, and DuckDB runs the statement inside the task over the Delta files. Nothing starts on Databricks, so there are no DBUs and no cold start. It bypasses the engine, so it cannot read a view, and it ignores row filters and column masks.

**Warehouse** sends the statement to a SQL warehouse through `query()`. There is no session or cluster shape to manage, and unlike vending the statement goes through the engine, where views, row filters, and column masks apply exactly as in the SQL editor.

## Reading the output

From a laptop against an Azure workspace, with a warm serverless warehouse:

```
path         step s  query s  groups  answer
baseline       19.8        -       -  reads nothing
connect        25.3      2.6      17  reference
job            75.6        -      17  same
vending        26.7      6.7      17  same
warehouse      24.5      4.2      17  same
```

`step s` runs from task creation to completion, so it includes whatever happens before the step body: `@spark` opens the Connect session, and submits the job and waits for it, before the body starts, which a timer in the body would miss. `query s` is the statement alone. The `baseline` branch reads nothing, so its time is what every task costs in this deployment, and the difference between it and a path's `step s` is what that path adds. Here the job's 55 extra seconds are serverless job startup, while the other three paths cost a few seconds each. On a cold warehouse or a cluster-backed Connect session those numbers change, which is why this is a flow to run on the customer's workspace rather than a table to quote.

The second command reads the view. Connect, job, and warehouse return the same 17 groups, since dropping `customer_id` does not change the aggregate, and vending reports why it cannot:

```
vending could not run: UnityCatalogError
main.retail.orders_public has no storage location, so it cannot be read without Spark. Views and foreign tables have to go through to_spark() or query().
```

## The comparison

| | Connect | Job | Vending | Warehouse |
| --- | --- | --- | --- | --- |
| Where the statement runs | Databricks serverless, driver in the task | a Databricks job | the Metaflow task | a SQL warehouse |
| Paid before it runs | session start | job compute start | nothing | warehouse wake-up |
| DBUs | yes | yes | none | SQL DBUs |
| Joins across large tables | yes | yes | not usefully | yes |
| Views, row filters, column masks | yes | yes | no | yes |
| Grants | `SELECT` | `SELECT`, plus `WRITE VOLUME` on the staging Volume | `SELECT` plus `EXTERNAL USE SCHEMA` | `SELECT`, `CAN USE` on the warehouse |
| Result arrives as | Arrow over Connect | Parquet staged on a Volume | Arrow from the Delta files | Arrow through external links |
| Cost attribution | session tag in query history | `custom_tags` in `system.billing.usage` | nothing to attribute | `query_tags` in `system.query.history` |

Vending wins when the read fits comfortably in the task's memory, roughly single-digit gigabytes, there is no view or row filter in the way, and the work afterwards is single-node anyway: feature transforms, training data assembly, evaluation. The warehouse wins over vending when a view, row filter, or column mask is involved, or the query joins tables the task cannot hold. Spark wins when the input is too large for a warehouse or the work is distributed beyond one aggregation, and job mode over Connect when the runtime has to be pinned or the work should survive the task.

## When a path is unavailable

Every branch carries `@catch(var="failure")`, so a path that is not available reports why and the others carry the run. Without a staging Volume, the job branch names the setting it needs. Without a warehouse id, so does the warehouse branch. Without `EXTERNAL USE SCHEMA`, which plenty of workspaces withhold deliberately, vending fails with an error naming the grant:

```
UnityCatalogError: Unity Catalog refused to vend credentials for main.retail.orders: ...
Credential vending requires EXTERNAL USE SCHEMA on the schema (or equivalent) in addition to
SELECT on the table. If your workspace does not allow it, read through Spark with to_spark()
instead.
```

## Deletion vectors

Deletion vectors are on by default on recent Databricks Runtimes for many write patterns, and `deltalake`'s Python reader cannot read a table that has them, as of 1.6.3. `to_arrow()`, and with it `to_pandas()`, `to_polars()`, and `to_duckdb()`, falls back to DuckDB's own Delta reader, which applies them correctly and honours the pin, so nothing in this flow has to handle it. Install `duckdb` to get the fallback.

The fallback covers AWS credentials and Azure SAS tokens, the two vended shapes that map onto a DuckDB secret. Both are verified against real workspaces, AWS including a deletion-vector table (see `tests/test_unity_catalog.py`). GCP has no DuckDB secret shape for the OAuth token Unity Catalog vends there, so a deletion-vector table on GCP fails with an explanation pointing at `to_spark()`.

## Source

### `four_ways.py`

```python
--8<-- "demos/02_reading/four_ways.py"
```

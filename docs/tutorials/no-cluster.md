# Reading Without a Cluster

```bash
pip install -e '../..[databricks,catalog]' duckdb
python three_ways.py run --table main.retail.orders --warehouse-id <id>
```

Reads `main.retail.orders`, the table demo 2 creates and mutates. If it does not exist yet, seed it
first: see "Seed data" in [`demos/02_unity_catalog/README.md`](../../demos/02_unity_catalog/README.md#seed-data).

`--warehouse-id` is optional; without it (or `DATABRICKS_WAREHOUSE_ID` in the environment), the SQL
warehouse branch is skipped and the demo still runs the other two.

The flow forks three ways, reads the same pinned Unity Catalog table on each branch, and prints wall
clock side by side. Two branches start nothing at all.

## Three ways to read the same table

These get conflated on calls, and the distinction decides where the money goes.

**Databricks Connect** is a Spark client. `self.spark` is a real session whose driver logic runs in
the Metaflow task and whose execution happens on Databricks compute. You get Databricks' Spark,
Photon, UC enforcement inside the engine, and the whole DataFrame API. You also get a running
cluster, or serverless compute billed for the duration, plus a cold start if nothing is warm.

**Credential vending** is not Spark at all. Unity Catalog checks the grant and issues a short-lived
credential scoped to that one table's storage location; the task then reads the Delta files directly
with delta-rs and does whatever it likes with the Arrow table, including DuckDB SQL, Polars, or
pandas. Governance is intact because UC issued the credential and can revoke the grant. There is no
cluster, no DBUs, and no cold start. Vending bypasses the engine, so it cannot see a view, a row
filter, or a column mask.

**A SQL warehouse statement**, via `query()`, is also not Spark, but unlike vending it goes through
the query engine: a view, a row filter, or a column mask applies exactly as it would in the SQL
editor. No cluster shape to configure, no session to detach before Metaflow persists artifacts, just
a statement and a warehouse id. The trade-off against vending is the same one SQL always has against
a direct file read: the engine can do more, and it costs a warehouse's DBUs to do it, though a
serverless warehouse's cold start is typically much shorter than a Spark cluster's.

So the question is never "which is more governed". It is "does this read need a distributed engine,
and if not, does it need the engine's enforcement at all".

| | Connect | Vending | Warehouse |
| --- | --- | --- | --- |
| Where the read runs | Databricks compute | the Metaflow task | a SQL warehouse |
| DBUs | yes | none | yes (SQL DBUs) |
| Cold start | seconds to minutes | none | none to tens of seconds (serverless) |
| Joins across large tables | yes | not usefully | yes |
| Predicate and column pushdown | yes | yes, via Delta and Arrow | yes |
| Views, foreign tables, row filters | yes | no, they need the engine | yes |
| Extra grant needed | `SELECT` | `SELECT` plus `EXTERNAL USE SCHEMA` | `SELECT` |
| Cost attribution | `custom_tags` on the cluster/job | none (no compute to tag) | `query_tags` on the statement |

Rules of thumb from the measured side: vending wins clearly when the read fits comfortably in the
task's memory, roughly single-digit gigabytes, when there is no view or row filter in the way, and
when the work afterwards is single-node anyway (feature transforms, training data assembly,
evaluation). The warehouse wins over vending when a view, row filter, or column mask is involved, or
when the query needs joins the task itself cannot do efficiently. Spark wins when the input is large
enough that even a warehouse struggles, or when the work is genuinely distributed beyond one
aggregation.

## The refusal case

`EXTERNAL USE SCHEMA` is a separate grant, and plenty of workspaces deliberately withhold it. When
that happens, vending fails with a `UnityCatalogError` naming the grant and pointing at `to_spark()`,
and `three_ways.py` catches it and lets the Spark branch carry the run. That is the honest demo: the
fallback is one line, and the customer's policy stays in charge of which path is available.

```
UnityCatalogError: Unity Catalog refused to vend credentials for main.retail.orders: ...
Credential vending requires EXTERNAL USE SCHEMA on the schema (or equivalent) in addition to
SELECT on the table. If your workspace does not allow it, read through Spark with to_spark()
instead.
```

## Deletion vectors are not a refusal, and mostly not even visible

Deletion vectors are on by default on recent Databricks Runtimes for many write patterns
(auto-compaction, predictive optimization), and `deltalake`'s own Python reader cannot read a table
that has them enabled at all, on any of its read methods, as of the latest release at the time this
was written (1.6.3). `to_arrow()` (and therefore `to_pandas()`, `to_polars()`, and `to_duckdb()`)
falls back to DuckDB's own Delta reader for that case, which does apply deletion vectors correctly,
and this happens automatically: nothing in this demo has to catch anything for it. Install `duckdb`
to get the fallback.

The fallback covers AWS and Azure SAS credentials, the two vended shapes that map onto a DuckDB
secret. AWS is verified end to end, including against a real deletion-vector table. Azure was
verified against a real Azure workspace and initially failed: the connection string needs
`AccountName=` in it, which Microsoft's own minimal `BlobEndpoint=...;SharedAccessSignature=...`
format for a SAS-based connection string does not include, but duckdb-azure's own pre-check requires
it and rejects the connection string before ever reaching the real Azure SDK parser without it.
Fixed. GCP has no DuckDB secret shape for the OAuth bearer token UC vends there, so it still falls
straight through to the explanation below.

A deletion-vector table on GCP, or an Azure one where credential vending itself is refused or the
account cannot be parsed from the storage location, still fails with an explanation naming the reader
feature and pointing at `to_spark()`, the same shape as the refusal case above, since both are genuine
edges of this path rather than something to paper over.

## Source

```python
--8<-- "demos/03_no_cluster/three_ways.py"
```

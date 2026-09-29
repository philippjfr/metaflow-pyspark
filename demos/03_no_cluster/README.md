# Demo 3: reading governed data without paying for a cluster

```bash
pip install -e '../..[databricks,catalog]' duckdb
python two_ways.py run --table main.retail.orders --warehouse-id <id>
```

Reads `main.retail.orders`, the table demo 2 creates and mutates. If it does not exist yet, seed it
first: see [Seed data](../02_unity_catalog/README.md#seed-data) in demo 2's README.

`--warehouse-id` is optional; without it (or `DATABRICKS_WAREHOUSE_ID` in the environment), the SQL
warehouse branch is skipped and the demo still runs the vending branch.

The flow forks two ways, reads the same Unity Catalog table on each branch, and prints wall clock
side by side. Neither branch starts a Spark cluster.

## Two ways to read the same table

**Credential vending** is not Spark at all. Unity Catalog checks the grant and issues a short-lived
credential scoped to that one table's storage location; the task then reads the Delta files directly
with delta-rs and does whatever it likes with the Arrow table, including DuckDB SQL, Polars, or
pandas. Governance is intact because UC issued the credential and can revoke the grant. There is no
compute to pay for and no cold start. Vending bypasses the engine, so it cannot see a view, a row
filter, or a column mask.

**A SQL warehouse statement**, via `query()`, goes through the query engine: a view, a row filter, or
a column mask applies exactly as it would in the SQL editor. No cluster shape to configure and no
session to manage, just a statement and a warehouse id. The trade-off against vending is the usual
one between an engine and a direct file read: the engine can do more, and it costs a warehouse's DBUs
to do it.

| | Vending | Warehouse |
| --- | --- | --- |
| Where the read runs | the Metaflow task | a SQL warehouse |
| DBUs | none | yes (SQL DBUs) |
| Cold start | none | none to tens of seconds (serverless) |
| Joins across large tables | not usefully | yes |
| Predicate and column pushdown | yes, via Delta and Arrow | yes |
| Views, foreign tables, row filters | no, they need the engine | yes |
| Extra grant needed | SELECT plus `EXTERNAL USE SCHEMA` | SELECT |
| Cost attribution | none (no compute to tag) | `query_tags` on the statement |

Vending wins when the read fits comfortably in the task's memory, there is no view or row filter in
the way, and the work afterwards is single-node anyway (feature transforms, training data assembly,
evaluation). The warehouse wins when a view, row filter, or column mask is involved, or when the
query needs joins the task itself cannot do efficiently.

## The refusal case

`EXTERNAL USE SCHEMA` is a separate grant, and plenty of workspaces deliberately withhold it. When
that happens, vending fails with an error naming the grant, and `two_ways.py` catches it and lets the
warehouse branch carry the run.

```
UnityCatalogError: Unity Catalog refused to vend credentials for main.retail.orders: ...
Credential vending requires EXTERNAL USE SCHEMA on the schema (or equivalent) in addition to
SELECT on the table. If your workspace does not allow it, read through Spark with to_spark()
instead.
```

## Deletion vectors

Deletion vectors are on by default on recent Databricks Runtimes for many write patterns, and
`deltalake`'s Python reader cannot read a table that has them enabled, on any of its read methods, as
of 1.6.3. `to_arrow()` (and therefore `to_pandas()`, `to_polars()`, and `to_duckdb()`) falls back to
DuckDB's own Delta reader for that case, at the pinned version, which applies deletion vectors
correctly. Install `duckdb` to get the fallback.

The fallback covers AWS and Azure SAS credentials, the two vended shapes that map onto a DuckDB
secret. Both have been run against real deletion-vector tables, including pinned reads of an older
version on Azure. GCP has no DuckDB
secret shape for the OAuth bearer token UC vends there, so a deletion-vector table on GCP fails with
an explanation naming the reader feature and pointing at `to_spark()`.

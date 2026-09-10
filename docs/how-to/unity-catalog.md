# Read & Write Unity Catalog Tables

`UnityCatalogTable` is a Metaflow artifact that is a **reference to a table, not a copy
of it**. Assigning one records the table's current Delta version, so a re-run reads the
same bytes even after the table has moved on — Metaflow's reproducibility guarantee,
extended over data the customer governs in Unity Catalog.

```python
from metaflow import UnityCatalogTable

@step
def start(self):
    self.orders = UnityCatalogTable("main.retail.orders")   # pins the current version
    self.next(self.crunch)
```

## Reading

Two ways to read, and the choice is "does this read need a distributed engine", not
"which one is more governed" — both go through UC's own grant checks.

**Through Spark**, honouring the pinned version, for large tables or anything
join-heavy:

```python
@spark(backend="databricks")
@step
def crunch(self):
    df = self.orders.to_spark(self.spark)   # reads versionAsOf the pinned version
```

**Without any cluster**, for everything else. Unity Catalog vends a temporary, scoped
credential for the table's storage location, and the read happens directly against the
Delta files with no Spark involved:

```python
@step
def crunch(self):
    df = self.orders.to_pandas()    # or .to_arrow(), .to_polars(), .to_duckdb()
```

Governance is intact either way: UC issues the credential and enforces the grant.
Vending needs `EXTERNAL USE SCHEMA` in addition to `SELECT`, and raises
`UnityCatalogError` naming the missing grant when it is absent, rather than surfacing an
opaque storage 403:

```
UnityCatalogError: Unity Catalog refused to vend credentials for main.retail.orders: ...
Credential vending requires EXTERNAL USE SCHEMA on the schema (or equivalent) in
addition to SELECT on the table. If your workspace does not allow it, read through
Spark with to_spark() instead.
```

Catch that specific case to fall back to `to_spark()` when a workspace does not allow
vending — see [Handle Failures & Cancellation](failures-and-cancellation.md).

## Pinning explicitly

`UnityCatalogTable` pins the current version at construction time by default. Pin a
specific version or timestamp instead when you already know which snapshot you want:

```python
self.orders = UnityCatalogTable("main.retail.orders", version=42)
self.orders = UnityCatalogTable("main.retail.orders", timestamp="2026-08-01T00:00:00Z")

older = self.orders.at_version(41)
current = self.orders.latest()
history = self.orders.history(limit=20)
```

## Writing, and lineage

Register a step's output back as a UC table, and stamp it with the Metaflow pathspec so
the customer's own lineage graph shows which flow produced it:

```python
df.write.mode("append").saveAsTable("main.retail.daily_summary")

from metaflow_extensions.spark.plugins.catalog.unity import UnityCatalogTable

table = UnityCatalogTable("main.retail.daily_summary")
tags = table.lineage_tags(ctx=current)   # metaflow_pathspec, metaflow_flow, metaflow_run_id
```

Open the table in the workspace and look at its tags panel to see the exact task that
produced it, or paste the pathspec into `Task("...")` from a notebook to get back to the
run.

## Rule of thumb: Spark vs. vending

| | Databricks Connect / Spark | Credential vending |
| --- | --- | --- |
| Where the read runs | Databricks compute | the Metaflow task |
| DBUs | yes | none |
| Cold start | seconds to minutes | none |
| Joins across large tables | yes | not usefully |
| Views, row filters, column masks | yes, engine-enforced | no — fails or falls back |
| Extra grant needed | `SELECT` | `SELECT` plus `EXTERNAL USE SCHEMA` |

Vending wins clearly when the read fits comfortably in the task's memory — roughly
single-digit gigabytes — and the work afterward is single-node anyway. Spark wins when
the input is large, the work is a shuffle or a join, or the table is a view or has
row-level security, since those are enforced by the engine and vending cannot see them.

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

Four ways to write a governed table, each ending in a pinned reference:

```python
# Spark, in a session step
df.write.mode("overwrite").saveAsTable("main.retail.daily_summary")
self.daily = UnityCatalogTable("main.retail.daily_summary")

# a submitted job: the artifact comes back pinned to the version the job wrote
@spark(backend="databricks", mode="job", job=summarize,
       output_table="main.retail.daily_summary", output_format="table",
       output_artifact="daily")

# a warehouse statement
query("CREATE OR REPLACE TABLE main.retail.daily_summary AS SELECT ...",
      output_format="none")
self.daily = UnityCatalogTable("main.retail.daily_summary")

# credential vending, external tables only
self.daily = UnityCatalogTable("main.ext.daily_summary").write(arrow_table, mode="append")
```

`write()` commits with `deltalake` through vended `READ_WRITE` credentials, so it needs `MODIFY` and `EXTERNAL USE SCHEMA`, and refuses a managed table, whose writes Databricks has to commit. The engine never sees a vended write, so Unity Catalog records no lineage for it and does not evaluate constraints or generated columns.

Then stamp the table with the task that wrote it:

```python
self.daily.stamp_lineage(current)   # metaflow_pathspec, metaflow_flow, metaflow_run_id
```

`stamp_lineage` runs `ALTER TABLE ... SET TAGS` through `query()`, so inside a `@spark` step it reuses the session and elsewhere it needs a warehouse. Open the table in the workspace and look at its tags panel to see the exact task that produced it, or paste the pathspec into `Task("...")` from a notebook to get back to the run.

## Choosing a path

Spark, credential vending, and a SQL warehouse compared on the same table, for reads and writes, is the subject of the [reading](../tutorials/reading.md) and [writing](../tutorials/writing.md) tutorials, including a four-way comparison table. In short: vending wins when the read fits comfortably in the task's memory, roughly single-digit gigabytes, and the work afterward is single-node anyway. A warehouse wins when a view, row filter, or column mask is involved. Spark wins when the input is large or the work is a shuffle or a join.

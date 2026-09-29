
# EXPERIMENTAL `@pyspark` decorator for Metaflow

see `example/sparkflow.py` for an example

## Reading Databricks data without a cluster

Two ways to read governed Databricks data from any Metaflow step, neither of which starts Spark compute.

```bash
pip install -e '.[catalog]' duckdb     # Unity Catalog tables via credential vending
pip install -e '.[databricks]'         # SQL warehouse statements
```

Authentication is delegated to `databricks-sdk`, so PATs, CLI profiles, OAuth service principals, and Azure MSI work as they already do (`DATABRICKS_HOST` and `DATABRICKS_TOKEN`, or `DATABRICKS_CONFIG_PROFILE`).

### Unity Catalog tables as artifacts

```python
from metaflow import UnityCatalogTable

self.orders = UnityCatalogTable("main.retail.orders")   # pins the current Delta version
```

The artifact is a reference, not a copy. Assignment records the table's current Delta version, and every read through the reference uses that version, so a re-run reads the same bytes even after the table has moved on. `at_version()`, `at_timestamp()`, and `latest()` return re-pinned references, and `history()` lists the versions.

`to_arrow()`, `to_pandas()`, `to_polars()`, and `to_duckdb()` ask Unity Catalog to vend temporary, table-scoped credentials and read the Delta files directly with `deltalake`. UC checks the grants and issues the credentials, and the credentials are never pickled into the artifact. Vending needs `EXTERNAL USE SCHEMA` in addition to `SELECT`, and the error says so when it is missing. Tables with deletion vectors, which `deltalake` cannot read, fall back to DuckDB's Delta reader on AWS and Azure. `to_spark(session)` reads the pinned version through a Spark session you already have.

### SQL warehouse statements

```python
from metaflow_extensions.spark.plugins.warehouse import query

self.daily = query(
    "SELECT order_date, SUM(amount) AS revenue FROM main.retail.orders "
    "WHERE order_date >= :since GROUP BY order_date",
    params={"since": self.since},
    output_format="polars",
)
```

`query()` submits one statement to a Databricks SQL warehouse and returns the result as `pandas`, `arrow`, `polars`, or `none`. `params` bind as named, typed parameters through `:name` markers and are never formatted into the SQL text. Unlike credential vending, a statement goes through the query engine, so views, row filters, and column masks apply. An arbitrary statement is not pinned the way a `UnityCatalogTable` is; add `VERSION AS OF` yourself when that matters.

The warehouse is `warehouse_id=`, else `warehouse_id` in the `databricks` section of a flow-level config artifact (`query(..., flow=self)` reads `self.spark_config`), else `METAFLOW_DATABRICKS_WAREHOUSE_ID` or `DATABRICKS_WAREHOUSE_ID`. Polling shares one wait loop that cancels the statement on interrupt or timeout and reports a control-plane outage as `SparkControlPlaneError` rather than as a failed statement. Each statement carries the flow, run, step, task, and user as `query_tags`, which land in `system.query.history` (needs `databricks-sdk>=0.86`; older SDKs run without the tags).

### Demos

[`demos/02_unity_catalog`](demos/02_unity_catalog) pins a table, mutates it, and replays the original version. [`demos/03_no_cluster`](demos/03_no_cluster) reads the same table through vending and through a warehouse, side by side.

### Tests

```bash
python -m pytest tests -q
```

No cloud account needed.

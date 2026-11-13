# Results: Copy or Reference

```bash
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
python results.py run
```

Chapter 2 compared ways of reading. This chapter holds the reader fixed, chapter 2's job function, and varies what comes back. Four branches submit the same job with a different `output_format`:

| Branch | `output_format` | The artifact holds |
| --- | --- | --- |
| `copy` | `pandas` | the daily aggregate as a DataFrame, pickled into Metaflow's datastore |
| `everything` | `pandas` | every row of the table, which is what `toPandas()` on an unaggregated read does |
| `url` | `url` | the path of the Parquet the job wrote to the staging Volume |
| `reference` | `table`, with `output_table=ORDERS_DAILY` | a `UnityCatalogTable` pinned to the version the job just wrote |

`compare` prints each artifact's pickled size, read from the datastore, and then reads each one back:

```
form            artifact  holds
copy               1.3KB  DataFrame, 17 rows
everything       115.0KB  DataFrame, 3333 rows
url                0.1KB  path /Volumes/main/metaflow/staging/metaflow/ResultsFlow/74191/url/145279/out
reference          0.5KB  UnityCatalogTable(main.retail.orders_daily @v8)
```

## Copy or reference

The two `pandas` branches are convenient: the next step has the data in hand and needs nothing else. They are also copies of governed data in a store Unity Catalog does not govern, and their size grows with the result. The seeded table is small enough that `everything` stays well under a megabyte; on a real table it is the branch that triggers the extension's warning:

```
UserWarning: @spark materialized 1.4GB into 'self.spark_df'. Consider
output_format='table' or 'url' to pass a reference to the next step instead of
copying the data.
```

`url` is small but untyped. The next step gets a path, needs read access to the staging Volume, and gets whatever is at that path when it reads, since nothing records a version.

`reference` is small and governed. The job writes `ORDERS_DAILY` with `saveAsTable`, reports the Delta version it committed, and the step's artifact is a `UnityCatalogTable` pinned to that version. Any later step, or a re-run of one, reads exactly what this job wrote, through Spark or through vending, while the table itself stays in Unity Catalog under its grants. That property is what chapter 5 builds on.

`url` and `table` only exist for submitted jobs, since a Connect session holds a live DataFrame rather than written output. In a session step the equivalent of `reference` is to write the table yourself and assign `UnityCatalogTable(name)`, which chapter 4 does.

## Source

### `results.py`

```python
--8<-- "demos/03_results/results.py"
```

# Choose an Output Format

`output_format` decides what a `@spark` step puts into its output artifact
(`self.spark_df` by default, renamed with `output_artifact=...`).

```python
@spark(backend="databricks", mode="job", job=jobs.etl.summarize,
       output_format="polars")
@step
def crunch(self):
    print(self.spark_result.head())
```

| Format | What lands in the artifact |
| --- | --- |
| `"pandas"` | a pandas `DataFrame` (the default) |
| `"arrow"` | a `pyarrow.Table` |
| `"polars"` | a `polars.DataFrame` |
| `"spark"` | the live Spark `DataFrame` itself — not picklable, only useful within the same step |
| `"table"` | a reference to the output table, not a copy |
| `"url"` | a storage URL pointing at the written output, not a copy |
| `"none"` | nothing; use this when the step only has side effects |

## Materializing vs. referencing

`"pandas"`, `"arrow"`, and `"polars"` **materialize** the result: the data is copied out
of Spark and into the task process. That is convenient, and it is the wrong default
past a certain size — a result over 1GB triggers a warning naming `"table"` or `"url"`
as the alternative.

```
UserWarning: @spark materialized 1.4GB into 'self.spark_df'. Consider
output_format='table' or 'url' to pass a reference to the next step instead of
copying the data.
```

`"table"` and `"url"` pass a **reference** instead. The next step receives something it
can read from — a table name or a storage location — rather than a copy of the data
itself, which is the shape you want once a result stops being small.

## `foreach` over Spark work

Fanning out over Spark jobs is the most common shape in practice, and it composes with
output formats the same way any other step does: each branch gets its own artifact,
readable independently once the `foreach` joins.

```python
@step
def start(self):
    self.regions = ["us", "eu", "apac"]
    self.next(self.crunch, foreach="regions")

@spark(backend="databricks", mode="job", job=jobs.etl.by_region,
       job_parameters=["region"], output_format="table")
@step
def crunch(self):
    self.next(self.join)
```

## Unity Catalog tables

A `UnityCatalogTable` artifact is a third shape, closer to `"table"` than to a
materializing format: it is a *pinned reference*, not a copy, and it survives the flow
without ever going through `output_format` at all. See
[Read & Write Unity Catalog Tables](unity-catalog.md).

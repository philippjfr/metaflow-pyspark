# Migrate from @pyspark

`@pyspark` was the original decorator this repo shipped, targeting EMR Serverless only.
`@spark` generalizes it to multiple backends; `@pyspark` is now kept as a
backwards-compatible alias so existing flows keep running unchanged.

## What stays the same

```python
@pyspark(job=myjob.run, job_parameters=["param"], output_artifact="spark_df")
@step
def start(self):
    print(len(self.spark_df))
```

still works exactly as before: `@pyspark` defaults `backend="emr-serverless"`, so a flow
that never mentions a backend keeps submitting to the same place it always did.
`example/sparkflow.py` in the repository is kept specifically as this compatibility
example.

## What is translated automatically

| Old attribute | Behaves as |
| --- | --- |
| `output_pandas=True` | `output_format="pandas"` |
| `output_pyarrow=True` | `output_format="arrow"` |
| `output_pandas=False, output_pyarrow=False` | `output_format="url"` |
| `user_timeout=...` | `timeout=...` |
| `spark_config=...` | `config=...` |

None of these need to change in an existing flow. They are documented here for anyone
moving a step from `@pyspark` to `@spark` explicitly and wanting the equivalent spelling.

## Why move to `@spark` anyway

`@pyspark` only ever reaches EMR Serverless, and only in job style: there is no session
mode, no Unity Catalog integration, and no compute shapes beyond whatever
`spark_config.json` describes. Moving to `@spark(backend=...)` gets you:

- [Session style](session-vs-job.md), for a fast iteration loop with no packaging step.
- [Unity Catalog tables](unity-catalog.md) as artifacts, with version pinning and
  credential-vended reads.
- A documented [cost attribution](cost-attribution.md) story, if the target is
  Databricks.
- The same [failure taxonomy](failures-and-cancellation.md) — cancellation, timeouts,
  and control-plane outages are now distinct rather than one bare exception.

The migration itself is one line: replace `@pyspark(...)` with
`@spark(backend="emr-serverless", ...)` to keep the same target explicitly, or drop the
`backend=` entirely and set it through config to move the flow between backends without
touching the decorator again.

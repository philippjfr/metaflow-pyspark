# Attribute Cost

Databricks bills compute. It has no way to know that a cluster-hour belonged to the
`features` step of a specific flow run, so every job `@spark` submits carries that
mapping as a provider tag:

```
metaflow_flow, metaflow_run_id, metaflow_step, metaflow_task_id,
metaflow_pathspec, metaflow_attempt, metaflow_user
```

plus anything you add yourself:

```python
@spark(backend="databricks", mode="job", job=jobs.etl.summarize,
       tags={"cost_center": "ml-platform"})
```

## Querying the spend back out

Those tags land in `system.billing.usage`. Group by them to turn a monthly total into a
per-flow, per-run, per-step breakdown:

```sql
SELECT
    custom_tags['metaflow_flow']   AS flow,
    custom_tags['metaflow_run_id'] AS run_id,
    custom_tags['metaflow_step']   AS step,
    custom_tags['metaflow_user']   AS user,
    SUM(usage_quantity)            AS dbus,
    SUM(usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS est_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND u.usage_end_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE custom_tags['metaflow_flow'] IS NOT NULL
  AND usage_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY ALL
ORDER BY est_cost_usd DESC
```

Billing rows take a few hours to appear after a run finishes — that is the usage
pipeline's own latency, not something to debug in the extension. The
[cost attribution tutorial](../tutorials/cost-attribution.md) wraps this query in a
runnable script with `--group-by flow|step|user` and a `--print-query` flag for pasting
into a workspace SQL editor.

## Tags need job-mode compute

`custom_tags` can only attach to compute a run creates, which means `mode="job"` (or
`emr-serverless`). In `mode="connect"` the session usually runs against compute the
extension did not start — a shared serverless endpoint or an existing cluster — so there
is no per-run compute to tag.

What Connect mode does instead is label the *session*: every query gets the Metaflow
pathspec as its job description and a session tag, so it is findable in Query History
and, where enabled, `system.query.history`. That gives per-query attribution to a
pathspec, but the DBU line item still belongs to the shared compute underneath it.

The practical rule: use `mode="job"` for the steps whose cost needs to be attributed
precisely, or give a team its own compute and attribute at that level. Connect mode is
the better developer experience; job mode is the better accounting.

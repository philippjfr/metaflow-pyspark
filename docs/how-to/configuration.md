# Configure Compute and Credentials

Where a `@spark` step runs is deliberately kept out of the step body, so the same flow
file moves between a laptop, staging, and production without being edited. Configuration
is resolved from four sources, lowest to highest precedence:

1. **Backend defaults** — for example, `backend="local"` and a 60-minute timeout.
2. **Environment variables, or the Metaflow config** — `METAFLOW_SPARK_*` and the
   standard `DATABRICKS_*` variables.
3. **A flow-level config artifact** — by default `self.spark_config`, as a dict, a JSON
   string, or an `IncludeFile`.
4. **Explicit decorator attributes** — `@spark(backend=..., serverless=True, ...)`.

Each source is deep-merged into the ones below it, so a config artifact can set
defaults for every step while one decorator overrides a single attribute.

## Environment variables

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
# or
export DATABRICKS_CONFIG_PROFILE=my-workspace

export METAFLOW_SPARK_BACKEND=databricks
export METAFLOW_SPARK_MODE=job
export METAFLOW_DATABRICKS_RUNTIME_VERSION=15.4.x-scala2.12
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
```

Authentication is delegated entirely to `databricks-sdk`, so PATs, CLI profiles, OAuth
service principals, and Azure MSI all work the way they already do outside Metaflow — no
credential mechanism is Metaflow-specific, and no credential ever has to appear in flow
source.

## A flow-level config artifact

```python
@step
def start(self):
    self.spark_config = {
        "backend": "databricks",
        "databricks": {"serverless": True, "runtime_version": "15.4.x-scala2.12"},
    }
    self.next(self.crunch)

@spark(backend="databricks")
@step
def crunch(self):
    ...
```

Point a different attribute name at the same idea with `@spark(config="my_config")`.
This is what makes one flow file portable across environments: point the artifact at a
different dict — or a different `IncludeFile` — per deployment target instead of editing
the decorator.

## Explicit decorator attributes

```python
@spark(backend="databricks", mode="job", serverless=True,
       runtime_version="15.4.x-scala2.12", photon=True)
@step
def crunch(self):
    ...
```

Attributes always win. Use them when a specific step genuinely needs different compute
than the rest of the flow — everything else should live in config so it can change
without a code review.

## Databricks connection attributes

| Attribute | Environment variable | Notes |
| --- | --- | --- |
| `host` | `DATABRICKS_HOST` | workspace URL |
| `token` | `DATABRICKS_TOKEN` | personal access token |
| `profile` | `DATABRICKS_CONFIG_PROFILE` | `~/.databrickscfg` profile |
| — | `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | OAuth service principal |
| `volume` | `METAFLOW_DATABRICKS_VOLUME` | Unity Catalog Volume for staged code (job mode) |
| `warehouse_id` | `METAFLOW_DATABRICKS_WAREHOUSE_ID` | SQL warehouse, where used |

Any authentication mechanism the Databricks SDK already understands works without
Metaflow-specific setup.

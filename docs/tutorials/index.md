# Tutorials

Runnable walkthroughs, one per demo in
[`demos/`](https://github.com/outerbounds/metaflow-pyspark/tree/main/demos). Each demo
answers a question that comes up in real conversations about Metaflow and
Databricks/Spark.

| Tutorial | Answers | Needs a Databricks account |
| --- | --- | --- |
| [Hello Spark: One Flow, Three Backends](hello-spark.md) | "is it real Spark", "how do I get started" | no, runs locally too |
| [Unity Catalog Governance](unity-catalog-governance.md) | governance, reproducibility over governed data | yes |
| [Reading Without a Cluster](no-cluster.md) | reading governed data without paying for a cluster | yes |
| [Orchestrating Existing Jobs](existing-jobs.md) | "do I have to rewrite my pipelines" | yes |
| [Cost Attribution](cost-attribution.md) | "why did this month cost more" | yes, plus system tables |

Not built yet: debugging/observability needs the Spark card, an MLflow bridge, a
notebook-triggered flow, a production-project reference, and an end-to-end composite.

## Setup

```bash
pip install -e '.[databricks,catalog]'
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

Any authentication the Databricks SDK understands works, including
`~/.databrickscfg` profiles and OAuth service principals:

```bash
export DATABRICKS_CONFIG_PROFILE=my-workspace
```

The first tutorial needs none of this — run it first. Demos that submit jobs need a
Unity Catalog Volume to stage code on:

```bash
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
```

Each demo directory has its own README with the exact commands; run flows from inside
their directory, since they import job modules sitting next to them.

## Which tutorial answers which objection

**"We already have Databricks, why add anything."** [Existing jobs](existing-jobs.md),
then [cost attribution](cost-attribution.md). Metaflow orchestrates what exists without
touching it, then shows where the money went.

**"Does this respect our governance."**
[Unity Catalog governance](unity-catalog-governance.md). Every read goes through Unity
Catalog, and Metaflow adds version pinning on top rather than copying data out.

**"Spark is expensive for what we do."**
[Reading without a cluster](no-cluster.md). Same table, same grants, no cluster.

**"Is this actually Spark."** [Hello Spark](hello-spark.md), in the three-backend form,
live.

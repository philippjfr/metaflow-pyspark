"""Chapter 7a: three compute shapes, one run, all of it attributable.

    export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
    export DEMO_INSTANCE_POOL_ID=0801-...-pool
    python tagged_runs.py run

Every job this extension submits carries `metaflow_flow`, `metaflow_run_id`,
`metaflow_step`, `metaflow_task_id`, `metaflow_pathspec`, `metaflow_attempt`, and
`metaflow_user` as Databricks `custom_tags`, plus anything passed to `@spark(tags=...)`.
Those land in `system.billing.usage`, which is what makes "why did this month cost more"
answerable per flow, per run, and per step. Run `query_costs.py` afterwards to see it.

The three steps submit chapter 2's daily aggregate to serverless, an instance pool, and
a fresh job cluster, so the cold-start question gets a measured answer too: the
`compare` step reads each run's `setup_duration` back from the Jobs API rather than
asserting anything. The aggregate is small on purpose: what differs between the shapes
is what they cost before the job's code runs. Credential vending starts no compute, so
it has no row here; chapter 2 timed it against these engines.

This uses `mode="job"` deliberately. Billing tags belong to the compute Databricks starts
for the run, so they are only available when the extension submits the job. Connect mode
attribution works differently, see the README.
"""

import os

from _env import step_env
from _orders import ORDERS, daily_revenue
from durations import run_durations

from metaflow import FlowSpec, Parameter, UnityCatalogTable, current, spark, step

RUNTIME = "15.4.x-scala2.12"
NODE_TYPE = os.environ.get("DEMO_NODE_TYPE", "i3.xlarge")
# The pool branch needs a real pool. Without DEMO_INSTANCE_POOL_ID it degenerates into a
# job cluster and the comparison loses its middle row.
INSTANCE_POOL_ID = os.environ.get("DEMO_INSTANCE_POOL_ID")


class TaggedRunsFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)

    @step_env("vending")
    @step
    def start(self):
        orders = UnityCatalogTable(self.table)
        self.table_name = orders.full_name
        self.version = orders.version
        self.next(self.on_serverless, self.on_pool, self.on_new_cluster)

    @step_env()
    @spark(
        backend="databricks",
        mode="job",
        job=daily_revenue,
        job_parameters=["table_name", "version"],
        serverless=True,
        output_format="pandas",
        tags={"shape": "serverless", "cost_center": "ml-platform"},
    )
    @step
    def on_serverless(self):
        self.shape = "serverless"
        self.spark_step = current.step_name
        self.next(self.compare)

    @step_env()
    @spark(
        backend="databricks",
        mode="job",
        job=daily_revenue,
        job_parameters=["table_name", "version"],
        instance_pool_id=INSTANCE_POOL_ID,
        num_workers=2,
        runtime_version=RUNTIME,
        output_format="pandas",
        tags={"shape": "instance-pool", "cost_center": "ml-platform"},
    )
    @step
    def on_pool(self):
        self.shape = "instance-pool"
        self.spark_step = current.step_name
        self.next(self.compare)

    @step_env()
    @spark(
        backend="databricks",
        mode="job",
        job=daily_revenue,
        job_parameters=["table_name", "version"],
        num_workers=2,
        node_type_id=NODE_TYPE,
        runtime_version=RUNTIME,
        output_format="pandas",
        tags={"shape": "new-cluster", "cost_center": "ml-platform"},
    )
    @step
    def on_new_cluster(self):
        self.shape = "new-cluster"
        self.spark_step = current.step_name
        self.next(self.compare)

    @step_env()
    @step
    def compare(self, inputs):
        self.run_id = current.run_id
        rows = []
        for branch in inputs:
            setup, execution, cleanup = run_durations(
                current.flow_name, current.run_id, branch.spark_step
            )
            rows.append((branch.shape, setup, execution, cleanup))

        print("\n%-20s %10s %10s %10s" % ("shape", "setup s", "exec s", "cleanup s"))
        for shape, setup, execution, cleanup in sorted(rows):
            print("%-20s %10.1f %10.1f %10.1f" % (shape, setup, execution, cleanup))
        print(
            "\nsetup is the cold start. Serverless and a warm pool avoid most of what a "
            "fresh job cluster pays before any of your code runs."
        )
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(
            "Now attribute the spend:\n"
            "    python query_costs.py --run-id %s" % self.run_id
        )


if __name__ == "__main__":
    TaggedRunsFlow()

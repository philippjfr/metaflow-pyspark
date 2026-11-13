"""Chapter 6b: the other direction, one job ported into a step.

    export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
    python ported_step.py run --as-of 2026-08-24

`orchestrate.py` triggers `customer-enrichment` as it stands. This flow contains the same
logic as a `@spark` step instead, so the two can be shown next to each other. What changes:

* the notebook or JAR becomes a plain Python function in `enrichment.py`, importable and
  unit-testable without a workspace
* parameters stop being a `dbutils.widgets` lookup and become function arguments
* the job still writes a governed table, as the original did, but the step gets back a
  reference pinned to the version it wrote (chapter 3), so the next step reads it
  without agreeing on a table name or racing tomorrow's run
* `python ported_step.py run` runs it, which means a change can be tried without deploying
  anything

What does not change: it is the same Spark, on the same compute, reading the same governed
tables. Porting is a code-organization move, not a re-platforming one, and it can happen
one job at a time while `@databricks_job` covers the rest.
"""

import enrichment
from _env import step_env
from _orders import ORDERS

from metaflow import FlowSpec, Parameter, spark, step

#: Read when the flow is defined, so a constant rather than a Parameter.
SEGMENTS = "main.retail.customer_segments"


class PortedStepFlow(FlowSpec):
    as_of = Parameter("as-of", default="2026-08-24")
    orders_table = Parameter("orders-table", default=ORDERS)

    @step_env("pyspark")
    @step
    def start(self):
        self.run_date = self.as_of
        self.orders_table_name = self.orders_table
        self.next(self.enrich)

    @step_env("pyspark", "vending")
    @spark(
        backend="databricks",
        mode="job",
        job=enrichment.enrich_customers,
        job_parameters=["run_date", "orders_table_name"],
        output_artifact="segments",
        output_format="table",
        output_table=SEGMENTS,
        write_mode="overwrite",
        tags={"migrated_from": "customer-enrichment"},
    )
    @step
    def enrich(self):
        print("wrote %r" % self.segments)
        self.next(self.end)

    @step_env("pyspark", "vending")
    @step
    def end(self):
        frame = self.segments.to_pandas()
        print(frame["segment"].value_counts().to_string())
        print("\n%d customers, read back at the version the job wrote" % len(frame))


if __name__ == "__main__":
    PortedStepFlow()

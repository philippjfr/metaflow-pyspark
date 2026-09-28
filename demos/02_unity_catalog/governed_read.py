"""Demo 2a: read governed data, write governed data, keep the pin.

    python governed_read.py run --table main.retail.orders

The interesting line is `UnityCatalogTable(self.table)` in `start`. It reads the table's
metadata through Unity Catalog and records the table's *current Delta version* on the
artifact. Nothing is copied: the artifact is a reference, so the run stays reproducible
without duplicating governed data into the Metaflow datastore.

If the caller lacks SELECT on the table, that line is where the run fails, with UC's own
error rather than a Spark stack trace. Try it with a profile for a user without the
grant:

    DATABRICKS_CONFIG_PROFILE=restricted python governed_read.py run
"""
from _env import step_env

from metaflow import (
    FlowSpec,
    Parameter,
    UnityCatalogTable,
    current,
    spark,
    step,
)


class GovernedReadFlow(FlowSpec):
    table = Parameter("table", default="main.retail.orders")
    output_table = Parameter("output-table", default="main.retail.orders_daily")

    @step_env("vending")
    @step
    def start(self):
        self.orders = UnityCatalogTable(self.table)
        print("pinned %r" % self.orders)
        print("storage location: %s" % self.orders.storage_location)
        self.next(self.summarize)

    @step_env("connect")
    @spark(backend="databricks", tags={"cost_center": "ml-platform"})
    @step
    def summarize(self):
        """Aggregate through Spark, honouring the pin, and write the result back to UC."""
        orders = self.orders.to_spark(self.spark)
        daily = (
            orders.groupBy("order_date")
            .agg({"order_id": "count", "amount": "sum"})
            .withColumnRenamed("count(order_id)", "orders")
            .withColumnRenamed("sum(amount)", "revenue")
            .orderBy("order_date")
        )
        daily.write.mode("overwrite").saveAsTable(self.output_table)

        # UC records table-level lineage for this write on its own. The tags are what
        # connect it back to the exact Metaflow task, which UC cannot infer.
        self.spark.sql(
            "ALTER TABLE %s SET TAGS ("
            "'metaflow_pathspec' = '%s', "
            "'metaflow_flow' = '%s', "
            "'metaflow_run_id' = '%s')"
            % (self.output_table, current.pathspec, current.flow_name, current.run_id)
        )

        self.daily = UnityCatalogTable(self.output_table)
        self.preview = daily.limit(20).toPandas()
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(self.preview.to_string(index=False))
        print("\nread    %r" % self.orders)
        print("wrote   %r" % self.daily)
        print(
            "\nBoth artifacts are references. Re-run reproduce.py to read this run's "
            "input at the version it saw."
        )


if __name__ == "__main__":
    GovernedReadFlow()

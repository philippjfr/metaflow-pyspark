"""Demo 3: the same governed table, read three ways.

    python three_ways.py run --table main.retail.orders

Reads `main.retail.orders`, the table demo 2 creates and mutates. If it does not exist
yet, seed it first: see "Seed data" in `../02_unity_catalog/README.md`.

All three branches read the same Unity Catalog table at the same pinned version, and all
are governed by UC. The difference is what runs the read:

*Through Spark* (`through_spark`): the query executes on Databricks compute. The right
answer for large tables and joins.

*Through credential vending* (`through_vending`): UC hands out a short-lived,
table-scoped cloud credential and the Metaflow task reads the Delta files itself, then
aggregates in DuckDB. No cluster starts, so there are no DBUs and no cold start. The right
answer for mid-size reads with no engine-enforced constructs in the way.

*Through a SQL warehouse* (`through_warehouse`): the same aggregate, submitted as a
statement to a warehouse via `query()`. Like vending, no Spark session or cluster-shape
config is involved; unlike vending, the statement goes through the query engine, so
views, row filters, and column masks are respected here. Needs `--warehouse-id`, or
`DATABRICKS_WAREHOUSE_ID` in the environment; the branch is skipped with an explanation
if neither is set, the same way the vending branch is skipped on a refused grant.

The point of running them side by side is that the crossover is a measurement, not an
opinion. Print the numbers for the customer's own table and the choice makes itself.
"""

import time

from _env import step_env

from metaflow import FlowSpec, Parameter, UnityCatalogTable, UnityCatalogError, spark, step
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError
from metaflow_extensions.spark.plugins.warehouse import query


class ThreeWaysFlow(FlowSpec):
    table = Parameter("table", default="main.retail.orders")
    warehouse_id = Parameter("warehouse-id", default=None)

    @step_env("vending")
    @step
    def start(self):
        self.orders = UnityCatalogTable(self.table)
        print("reading %r" % self.orders)
        self.next(self.through_vending, self.through_spark, self.through_warehouse)

    @step_env("vending")
    @step
    def through_vending(self):
        """No cluster. UC vends credentials, delta-rs reads, DuckDB aggregates."""
        started = time.monotonic()
        try:
            connection = self.orders.to_duckdb(view_name="orders")
        except UnityCatalogError as exc:
            # Three different things land here, and the message names which one:
            # EXTERNAL USE SCHEMA missing (a workspace policy choice), a view/row
            # filter/column mask (vending cannot see engine-enforced constructs at
            # all), or deletion vectors on a GCP table (the DuckDB fallback that
            # handles deletion vectors elsewhere covers AWS and Azure, not GCP).
            # None of these are bugs; they are this path's real edges, so say so and
            # let the Spark branch carry the run.
            print("credential vending refused:\n%s" % exc)
            self.vended = None
            self.next(self.compare)
            return

        self.vended = connection.execute(
            """
            SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
            FROM orders GROUP BY order_date ORDER BY order_date
            """
        ).df()
        self.vended_seconds = time.monotonic() - started
        self.vended_rows = len(self.vended)
        print("vended read in %.1fs, %d groups" % (self.vended_seconds, self.vended_rows))
        self.next(self.compare)

    @step_env("connect")
    @spark(backend="databricks", tags={"read_path": "spark"})
    @step
    def through_spark(self):
        """The same aggregate, executed on Databricks compute."""
        from pyspark.sql import functions as F

        started = time.monotonic()
        df = self.orders.to_spark(self.spark)
        self.spark_result = (
            df.groupBy("order_date")
            .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
            .orderBy("order_date")
            .toPandas()
        )
        self.spark_seconds = time.monotonic() - started
        print("spark read in %.1fs" % self.spark_seconds)
        self.next(self.compare)

    @step_env()
    @step
    def through_warehouse(self):
        """The same aggregate, submitted as a statement to a SQL warehouse.

        No cluster, no Spark session, same as vending; unlike vending, this goes
        through the query engine, so it works identically on a view or a table with a
        row filter. The table name is part of the trusted statement text rather than a
        bound parameter, because Databricks' parameter markers bind *values*, not
        identifiers; `self.table` is an operator-supplied flow Parameter, not
        attacker-controlled input, which is the same trust boundary `UnityCatalogTable`
        already relies on for the same string.
        """
        started = time.monotonic()
        try:
            self.warehouse_result = query(
                """
                SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
                FROM {table}
                GROUP BY order_date ORDER BY order_date
                """.format(table=self.table),
                warehouse_id=self.warehouse_id,
                output_format="pandas",
            )
        except SparkConfigError as exc:
            # No warehouse configured. Skip rather than fail: this branch is an
            # addition to the comparison, not a requirement to run the demo.
            print("SQL warehouse branch skipped:\n%s" % exc)
            self.warehouse_result = None
            self.next(self.compare)
            return
        self.warehouse_seconds = time.monotonic() - started
        print(
            "warehouse read in %.1fs, %d groups"
            % (self.warehouse_seconds, len(self.warehouse_result))
        )
        self.next(self.compare)

    @step_env()
    @step
    def compare(self, inputs):
        self.merge_artifacts(inputs, include=["orders"])
        vended = [i for i in inputs if hasattr(i, "vended_seconds")]
        sparked = [i for i in inputs if hasattr(i, "spark_seconds")]
        warehoused = [i for i in inputs if getattr(i, "warehouse_result", None) is not None]

        print("\n%-22s %10s %10s" % ("path", "seconds", "groups"))
        for label, taken, rows in (
            [("credential vending", i.vended_seconds, i.vended_rows) for i in vended]
            + [
                ("databricks spark", i.spark_seconds, len(i.spark_result))
                for i in sparked
            ]
            + [
                ("sql warehouse", i.warehouse_seconds, len(i.warehouse_result))
                for i in warehoused
            ]
        ):
            print("%-22s %10.1f %10d" % (label, taken, rows))

        print(
            "\nWall clock is measured here. The DBU side of the comparison shows up in "
            "the billing tables tagged with this run, which is demo 6: the vended read "
            "contributes nothing to it, and the warehouse read is attributed by "
            "query_tags rather than custom_tags, so it needs its own drill-down query."
        )
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        pass


if __name__ == "__main__":
    ThreeWaysFlow()

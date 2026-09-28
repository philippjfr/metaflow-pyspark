"""Demo 2b: move the table on, so the pin has something to prove.

    python mutate_orders.py run --table main.retail.orders

Appends a day of rows, which bumps the table's Delta version. Run this between
`governed_read.py` and `reproduce.py`.
"""

from _env import step_env

from metaflow import FlowSpec, Parameter, spark, step


class MutateOrdersFlow(FlowSpec):
    table = Parameter("table", default="main.retail.orders")
    rows = Parameter("rows", default=1000)

    @step_env("connect")
    @spark(backend="databricks")
    @step
    def start(self):
        before = self.spark.sql("DESCRIBE HISTORY %s LIMIT 1" % self.table).collect()[0]
        print("version before: %s" % before["version"])

        self.spark.sql(
            """
            INSERT INTO {table}
            SELECT
                'demo-' || CAST(id AS STRING)      AS order_id,
                CURRENT_DATE()                     AS order_date,
                ROUND(rand(7) * 500, 2)            AS amount
            FROM range({rows})
            """.format(table=self.table, rows=self.rows)
        )

        after = self.spark.sql("DESCRIBE HISTORY %s LIMIT 1" % self.table).collect()[0]
        self.version = after["version"]
        print("version after:  %s" % self.version)
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print("table is now at v%s; earlier runs still point at their own version"
              % self.version)


if __name__ == "__main__":
    MutateOrdersFlow()

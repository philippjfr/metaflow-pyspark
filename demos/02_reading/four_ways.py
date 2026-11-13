"""Chapter 2: one statement, four engines.

    python four_ways.py run --warehouse-id <id>
    python four_ways.py run --warehouse-id <id> --table main.retail.orders_public

All four branches run `DAILY_SQL` from `_orders.py` against the same pinned table
and must return the same answer. What differs is what runs the statement and what that
costs:

* `connect`: a Spark Connect session; the driver logic stays in this task, execution
  happens on Databricks serverless.
* `job`: the same statement as a job function, submitted to Databricks and read back
  from Parquet staged on a Volume.
* `vending`: Unity Catalog vends a short-lived credential for the table's files, and
  DuckDB runs the statement inside this task. Nothing starts on Databricks.
* `warehouse`: the statement goes to a SQL warehouse through `query()`.

A fifth branch, `baseline`, reads nothing, so its duration is what any task costs
before a path adds its own. Every branch carries `@catch`, so a path that is not
available (no Volume for the job, a refused vending grant, no warehouse) reports why
instead of failing the run. The second command reads a view, which only vending cannot
do.
"""

import time

from _env import step_env
from _orders import DAILY_SQL, ORDERS, daily_revenue, same_answer, source

from metaflow import (
    FlowSpec,
    Parameter,
    Step,
    UnityCatalogTable,
    catch,
    current,
    spark,
    step,
)
from metaflow_extensions.spark.plugins.warehouse import query

PATHS = ("connect", "job", "vending", "warehouse")


class FourWaysFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)
    warehouse_id = Parameter("warehouse-id", default=None)

    @step_env("vending")
    @step
    def start(self):
        self.orders = UnityCatalogTable(self.table)
        # Plain values for the job branch: its parameters are pickled for a cluster
        # that does not have this extension installed.
        self.table_name = self.orders.full_name
        self.version = self.orders.version
        print("reading %r" % self.orders)
        self.next(self.baseline, self.connect, self.job, self.vending, self.warehouse)

    @step_env()
    @step
    def baseline(self):
        """Reads nothing, so its duration is what any task costs in this deployment."""
        self.next(self.compare)

    @step_env("connect")
    @catch(var="failure")
    @spark(backend="databricks", tags={"read_path": "connect"})
    @step
    def connect(self):
        started = time.monotonic()
        statement = DAILY_SQL.format(source=source(self.table_name, self.version))
        self.result = self.spark.sql(statement).toPandas()
        self.query_seconds = time.monotonic() - started
        self.next(self.compare)

    @step_env()
    @catch(var="failure")
    @spark(
        backend="databricks",
        mode="job",
        job=daily_revenue,
        job_parameters=["table_name", "version"],
        output_artifact="result",
        output_format="pandas",
        tags={"read_path": "job"},
    )
    @step
    def job(self):
        self.next(self.compare)

    @step_env("vending")
    @catch(var="failure")
    @step
    def vending(self):
        started = time.monotonic()
        connection = self.orders.to_duckdb(view_name="orders")
        self.result = connection.execute(DAILY_SQL.format(source="orders")).df()
        self.query_seconds = time.monotonic() - started
        self.next(self.compare)

    @step_env()
    @catch(var="failure")
    @step
    def warehouse(self):
        started = time.monotonic()
        self.result = query(
            DAILY_SQL.format(source=source(self.table_name, self.version)),
            warehouse_id=self.warehouse_id,
            tags={"read_path": "warehouse"},
        )
        self.query_seconds = time.monotonic() - started
        self.next(self.compare)

    @step_env()
    @step
    def compare(self, inputs):
        self.merge_artifacts(inputs, include=["orders"])
        reference = None
        self.timings = {}
        failures = []

        print(
            "\n%-10s %8s %8s %7s  %s"
            % ("path", "step s", "query s", "groups", "answer")
        )
        print(
            "%-10s %8.1f %8s %7s  reads nothing"
            % ("baseline", _seconds("baseline"), "-", "-")
        )
        for name in PATHS:
            branch = getattr(inputs, name)
            step_seconds = _seconds(name)
            query_seconds = getattr(branch, "query_seconds", None)
            self.timings[name] = (step_seconds, query_seconds)

            failure = getattr(branch, "failure", None)
            if failure is not None:
                failures.append((name, failure))
                print(
                    "%-10s %8.1f %8s %7s  unavailable" % (name, step_seconds, "-", "-")
                )
                continue

            if reference is None:
                reference, answer = branch.result, "reference"
            else:
                answer = (
                    "same" if same_answer(reference, branch.result) else "DIFFERENT"
                )
            print(
                "%-10s %8.1f %8s %7d  %s"
                % (
                    name,
                    step_seconds,
                    "-" if query_seconds is None else "%.1f" % query_seconds,
                    len(branch.result),
                    answer,
                )
            )

        for name, failure in failures:
            # The reason is what matters on screen, not the traceback.
            print(
                "\n%s could not run: %s\n%s"
                % (name, failure.type.rsplit(".", 1)[-1], failure.exception.strip())
            )
        print(
            "\nstep s runs from task creation to completion, so beyond the baseline "
            "every task pays, it includes opening the session, starting the job's "
            "compute, vending credentials, or waking the warehouse. query s is the "
            "statement alone."
        )
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        pass


def _seconds(step_name):
    # Task timestamps rather than a clock in the step body: @spark opens the session
    # and submits the job before the body runs, and that is part of the cost.
    task = Step("%s/%s/%s" % (current.flow_name, current.run_id, step_name)).task
    return (task.finished_at - task.created_at).total_seconds()


if __name__ == "__main__":
    FourWaysFlow()

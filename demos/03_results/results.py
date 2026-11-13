"""Chapter 3: what a Spark step hands to the next one.

    export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
    python results.py run

The same job function from chapter 2, submitted four times with a different
`output_format`, so the only thing that changes is what lands in the artifact:

* `copy`: the aggregate as a pandas DataFrame, pickled into Metaflow's datastore.
* `everything`: every row of the table as a DataFrame, which is what `toPandas()` on an
  unaggregated read does. At real sizes this is where the 1GB warning fires.
* `url`: the Parquet the job wrote to the staging Volume, as a path. Small, but untyped
  and unpinned, and only readable by something with access to that Volume.
* `reference`: the job writes `ORDERS_DAILY` with `saveAsTable`, and the artifact is a
  `UnityCatalogTable` pinned to the version the job wrote. Small, governed, and a
  re-run of the next step reads the same bytes.

`compare` prints each artifact's pickled size from the datastore and then reads each
one back, which is where the difference between a copy and a reference shows.
"""

from _env import step_env
from _orders import ORDERS, ORDERS_DAILY, daily_revenue, whole_table

from metaflow import FlowSpec, Parameter, Step, UnityCatalogTable, current, spark, step

FORMS = ("copy", "everything", "url", "reference")
JOB = dict(
    backend="databricks",
    mode="job",
    job_parameters=["table_name", "version"],
    output_artifact="result",
)


class ResultsFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)

    @step_env("vending")
    @step
    def start(self):
        orders = UnityCatalogTable(self.table)
        self.table_name = orders.full_name
        self.version = orders.version
        print("reading %r" % orders)
        self.next(self.copy, self.everything, self.url, self.reference)

    @step_env()
    @spark(job=daily_revenue, output_format="pandas", **JOB)
    @step
    def copy(self):
        self.next(self.compare)

    @step_env()
    @spark(job=whole_table, output_format="pandas", **JOB)
    @step
    def everything(self):
        self.next(self.compare)

    @step_env()
    @spark(job=daily_revenue, output_format="url", **JOB)
    @step
    def url(self):
        self.next(self.compare)

    # vending, for the fallback where the job could not report the version it wrote and
    # the reference has to pin itself.
    @step_env("vending")
    @spark(
        job=daily_revenue,
        output_format="table",
        output_table=ORDERS_DAILY,
        write_mode="overwrite",
        **JOB,
    )
    @step
    def reference(self):
        self.next(self.compare)

    @step_env("vending")
    @step
    def compare(self, inputs):
        print("\n%-11s %12s  %s" % ("form", "artifact", "holds"))
        self.sizes = {}
        for name in FORMS:
            task = Step("%s/%s/%s" % (current.flow_name, current.run_id, name)).task
            self.sizes[name] = task["result"].size
            print(
                "%-11s %10.1fKB  %s"
                % (
                    name,
                    self.sizes[name] / 1024.0,
                    _describe(getattr(inputs, name).result),
                )
            )

        # Reading each one back is where the forms really differ.
        daily = inputs.reference.result
        print("\ncopy        already here, %d rows" % len(inputs.copy.result))
        print("everything  already here, %d rows" % len(inputs.everything.result))
        print("url         a path; reading it needs access to the staging Volume")
        print(
            "reference   %d rows read through vending at v%s, whatever the table "
            "has become since" % (daily.to_arrow().num_rows, daily.version)
        )
        self.daily = daily
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(
            "\nThe reference is the only form that is small, governed by Unity "
            "Catalog, and pinned. %r is what chapter 5 builds on." % self.daily
        )


def _describe(value):
    if isinstance(value, str):
        return "path %s" % value
    if isinstance(value, UnityCatalogTable):
        return repr(value)
    return "%s, %d rows" % (type(value).__name__, len(value))


if __name__ == "__main__":
    ResultsFlow()

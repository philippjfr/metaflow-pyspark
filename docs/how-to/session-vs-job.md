# Session or Job Style

`@spark` steps are written one of two ways, chosen by whether a `job=` callable is
given. Which one you can use depends on the backend.

## Session style

The step body uses Spark directly, and `self.spark` is a live session:

```python
@spark(backend="databricks")
@step
def features(self):
    df = self.spark.read.table("main.retail.orders")
    self.daily = df.groupBy("order_date").count().toPandas()
    self.next(self.end)
```

This is the only shape where local variables in the step are directly usable inside the
Spark code, and the only shape available on **session** backends (`local`, and
`databricks` without `mode="job"`). There is no packaging step, so it is the fastest
loop to iterate in.

The session is not picklable, so `self.spark` only exists for the duration of the step.
It is set before the step body runs and removed afterward, before Metaflow persists the
task's artifacts — do not store it on `self` yourself.

## Job style

A separate function receives a session and its return value becomes an artifact:

```python
@spark(backend="databricks", mode="job", job=jobs.etl.summarize,
       job_parameters=["cutoff"])
@step
def crunch(self):
    print(self.spark_df.head())
```

```python
# jobs/etl.py
def summarize(spark, cutoff):
    df = spark.read.table("main.retail.orders")
    return df.filter(df.amount >= cutoff).groupBy("order_date").count()
```

This is **required** on submit-style backends (`mode="job"`, `mode="existing"`,
`emr-serverless`): the driver has to run on the remote compute, so there is no
process for a step body to run inside. It is also the shape that lets Spark code live
in its own importable, testable module, independent of the flow.

Rules that follow from where the code runs:

- **The job function must not import `metaflow`.** It runs on the cluster, which does
  not have a Metaflow task around it.
- **The job's whole package ships, not just one file.** If `jobs/etl.py` imports from
  `jobs/helpers.py`, the whole `jobs/` package is packaged and shipped together. Add
  anything else the job needs with `include=[...]`.
- **Named parameters, not globals.** `job_parameters=["cutoff"]` reads `self.cutoff` off
  the flow at submit time and passes it as a keyword argument, so the job function's
  signature documents what it needs.

## Choosing between them

Use session style while developing, and whenever the backend supports it — there is no
packaging step and no pickle round trip to go wrong. Move to job style only when the
backend requires it, or when you need something a Connect session cannot give you: a
pinned runtime, Photon, cluster-scoped libraries, or JVM UDFs. See
[Choose a Backend](choose-a-backend.md) for the full list of when each is required.

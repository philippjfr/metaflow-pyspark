# Handle Failures & Cancellation

Job state comes back classified rather than as a single generic exception, so a `@spark`
step failing does not mean going to the provider's UI to find out why.

## The exception taxonomy

| Exception | Raised when |
| --- | --- |
| `SparkJobFailed` | the remote job reached a terminal failure state |
| `SparkJobTimeout` | the job did not finish within its timeout |
| `SparkJobCancelled` | the job was cancelled — usually because the flow was interrupted |
| `SparkControlPlaneError` | the provider's API was unreachable; the job's own state is unknown |
| `SparkConfigError` | the decorator or config was invalid before anything was submitted |
| `UnityCatalogError` | Unity Catalog refused a metadata call, a read, or a credential vend |

`SparkControlPlaneError` is deliberately distinct from `SparkJobFailed`: a transient
outage in the compute provider's control plane is not the same event as a Spark job
actually failing, and should not be reported to the user as one.

`SparkJobFailed` carries the provider's own error class and the tail of the driver log,
so the exception message is often enough on its own:

```
Spark job failed with state 'FAILED'.
Error class: INVALID_SCHEMA
Message: cannot resolve column `order_date`
Run details: https://<workspace>/jobs/.../runs/...
Spark UI: https://<workspace>/.../sparkui/...

Last 40 lines of driver output:
    ...
```

## Catching a specific failure

```python
from metaflow import UnityCatalogError

@step
def crunch(self):
    try:
        df = self.orders.to_pandas()
    except UnityCatalogError:
        # workspace does not allow credential vending; fall back to Spark
        df = self.orders.to_spark(self.spark).toPandas()
```

## Cancellation is a contract, not a feature

Interrupting a flow, or hitting a `@timeout`, cancels the remote job rather than leaving
it running and billing:

```python
@timeout(minutes=45)
@retry(times=2)
@databricks_job(job_name="raw-orders-ingest")
@step
def ingest(self):
    ...
```

`@timeout` interrupts the waiting task, which the backend translates into a cancel call
against the remote run. `@retry` resubmits from scratch. Both are ordinary Metaflow
decorators — nothing Databricks-specific is required to get SLA parity with a scheduler
that already has retries and timeouts.

## Inspecting a finished run later

Every step records its backend, remote run id, run URL, Spark UI URL, and a serialized
job handle as task metadata, so a run that finished last week stays inspectable:

```python
from metaflow import Task

Task("MyFlow/42/features/1").metadata_dict["spark-job-url"]
```

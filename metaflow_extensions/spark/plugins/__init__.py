"""Metaflow extension for Spark and governed Databricks data.

`@pyspark` submits a job to EMR Serverless. `UnityCatalogTable` and `query()` are plain
objects and functions usable from any step. Nothing here imports boto3, databricks-sdk,
deltalake, or pyarrow until a step actually uses them.
"""

STEP_DECORATORS_DESC = [("pyspark", ".pyspark_decorator.PySparkDecorator")]

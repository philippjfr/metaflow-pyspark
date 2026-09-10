"""Metaflow extension registering the Spark step decorators.

The modules are imported lazily by Metaflow through the "module.Class" strings below, so
none of the optional dependencies (pyspark, databricks-sdk, boto3, deltalake) are
imported unless a flow actually uses the corresponding backend.
"""

STEP_DECORATORS_DESC = [
    ("spark", ".decorator.SparkDecorator"),
    ("pyspark", ".decorator.PySparkDecorator"),
    ("databricks_job", ".databricks_decorators.DatabricksJobDecorator"),
    ("databricks_notebook", ".databricks_decorators.DatabricksNotebookDecorator"),
]

__mf_promote_submodules__ = ["spark"]

"""The `customer-enrichment` job, as importable Python.

This is what the ported Databricks job looks like once it is a function: it takes a session
and its parameters, returns a DataFrame, and knows nothing about Metaflow. It runs
unchanged under `@spark`, under `pytest` with a local session, and in a notebook.

Columns match `main.retail.orders` as chapter 0 seeds it.
"""

from pyspark.sql import functions as F


def enrich_customers(spark, run_date, orders_table_name):
    orders = spark.read.table(orders_table_name).where(F.col("order_date") <= run_date)

    return (
        orders.groupBy("customer_id")
        .agg(
            F.count("order_id").alias("orders"),
            F.sum("amount").alias("lifetime_value"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn(
            "segment",
            F.when(F.col("lifetime_value") > 5000, "high")
            .when(F.col("lifetime_value") > 500, "mid")
            .otherwise("low"),
        )
        .withColumn("as_of", F.lit(run_date))
    )

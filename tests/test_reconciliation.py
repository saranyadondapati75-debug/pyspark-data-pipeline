
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count


def test_order_counts_and_city_sales_reconcile():
    spark = (
        SparkSession.builder
        .appName("ReconciliationTests")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Small, predictable sample dataset.
        orders = spark.createDataFrame(
            [
                ("O001", "C001", 100.0),
                ("O002", "C001", 50.0),
                ("O003", "C002", 200.0),
            ],
            ["order_id", "customer_id", "amount"],
        )

        customers = spark.createDataFrame(
            [
                ("C001", "Anu", "Hyderabad"),
                ("C002", "Ravi", "Vijayawada"),
            ],
            ["customer_id", "customer_name", "city"],
        )

        enriched = orders.join(
            customers,
            on="customer_id",
            how="inner",
        )

        city_report = (
            enriched.groupBy("city")
            .agg(
                spark_sum("amount").alias("total_sales"),
                count("order_id").alias("order_count"),
            )
        )

        # Reconcile total records and total sales.
        assert enriched.count() == orders.count()
        assert (
            city_report.agg(spark_sum("order_count")).first()[0]
            == orders.count()
        )
        assert (
            city_report.agg(spark_sum("total_sales")).first()[0]
            == orders.agg(spark_sum("amount")).first()[0]
        )

    finally:
        spark.stop()

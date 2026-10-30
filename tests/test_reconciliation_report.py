
from pyspark.sql import SparkSession

from src.reconciliation import build_reconciliation_report


def test_reconciliation_report_counts_match():
    spark = (
        SparkSession.builder
        .appName("ReconciliationReportTest")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        valid_orders = spark.createDataFrame(
            [("O001", 100.0), ("O002", 50.0)],
            ["order_id", "amount"],
        )

        rejected_orders = spark.createDataFrame(
            [("O003", "unknown_customer")],
            ["order_id", "reject_reason"],
        )

        report = build_reconciliation_report(
            spark=spark,
            input_count=3,
            valid_orders=valid_orders,
            rejected_orders=rejected_orders,
        )

        row = report.first()

        assert row["input_count"] == 3
        assert row["valid_count"] == 2
        assert row["rejected_count"] == 1
        assert row["counts_reconcile"] is True

    finally:
        spark.stop()

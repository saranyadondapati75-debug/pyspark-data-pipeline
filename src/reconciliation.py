
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame


def build_reconciliation_report(
    spark: SparkSession,
    input_count: int,
    valid_orders: DataFrame,
    rejected_orders: DataFrame,
    malformed_count: int = 0,
    run_id: str = "unknown",
    start_time: str = "",
    end_time: str = "",
    duration_seconds: float = 0.0,
) -> DataFrame:

    valid_count = valid_orders.count()
    rejected_count = rejected_orders.count()

    counts_reconcile = (
        input_count
        == valid_count + rejected_count + malformed_count
    )

    report_row = [(
        run_id,
        start_time,
        end_time,
        datetime.now(timezone.utc).isoformat(),
        duration_seconds,
        input_count,
        valid_count,
        rejected_count,
        malformed_count,
        counts_reconcile,
    )]

    columns = [
        "run_id",
        "start_time_utc",
        "end_time_utc",
        "report_timestamp_utc",
        "duration_seconds",
        "input_count",
        "valid_count",
        "rejected_count",
        "malformed_count",
        "counts_reconcile",
    ]

    return spark.createDataFrame(report_row, columns)

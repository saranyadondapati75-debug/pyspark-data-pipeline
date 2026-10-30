
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.schema_validation import validate_input_schema
from src.order_validation import validate_orders
from src.reconciliation import build_reconciliation_report
from src.pipeline_logging import create_run_logger
from src.csv_preprocessing import split_csv_records

from src.config import (
    CUSTOMERS_FILE,
    ORDERS_FILE,
    VALID_ORDERS_PATH,
    REJECTED_ORDERS_PATH,
    MALFORMED_ORDERS_PATH,
    RECONCILIATION_REPORT_PATH,
)


def main():
    logger, run_id = create_run_logger()
    start_time = datetime.now(timezone.utc)
    spark = None

    logger.info("Starting order validation pipeline | run_id=%s", run_id)

    try:
        # Step 1: Preprocess the original CSV without modifying it
        data_dir = Path(ORDERS_FILE).parent
        clean_file = data_dir / "clean_orders_preprocessed.csv"
        malformed_file = data_dir / "malformed_orders_preprocessed.csv"

        clean_count, malformed_count = split_csv_records(
            ORDERS_FILE,
            clean_file,
            malformed_file,
        )

        input_count = clean_count + malformed_count

        logger.info("Input order count: %s", input_count)
        logger.info("Preprocessed valid records: %s", clean_count)
        logger.info("Malformed order count: %s", malformed_count)

        # Step 2: Start Spark
        spark = (
            SparkSession.builder
            .appName("YottaFlexOrderValidation")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        customers = (
            spark.read
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(str(CUSTOMERS_FILE))
        )

        orders_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_timestamp", StringType(), True),
        ])

        clean_orders = (
            spark.read
            .schema(orders_schema)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(str(clean_file))
        )

        malformed_schema = StructType([
            StructField("reason", StringType(), True),
            StructField("raw_record", StringType(), True),
        ])

        malformed_orders = (
            spark.read
            .schema(malformed_schema)
            .option("header", True)
            .csv(str(malformed_file))
        )

        # Step 3: Validate and classify orders
        validate_input_schema(customers, clean_orders)
        logger.info("Input schema validation passed.")

        valid_orders, rejected_orders = validate_orders(
            customers,
            clean_orders,
        )

        valid_count = valid_orders.count()
        rejected_count = rejected_orders.count()

        logger.info("Valid order count: %s", valid_count)
        logger.info("Rejected order count: %s", rejected_count)

        # Step 4: Reconcile all records
        if input_count != valid_count + rejected_count + malformed_count:
            raise ValueError(
                "Reconciliation failed: input count does not equal "
                "valid + rejected + malformed."
            )

        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()

        report = build_reconciliation_report(
            spark,
            input_count,
            valid_orders,
            rejected_orders,
            malformed_count=malformed_count,
            run_id=run_id,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration_seconds,
        )

        # Step 5: Save outputs
        valid_orders.write.mode("overwrite").parquet(
            str(VALID_ORDERS_PATH)
        )

        rejected_orders.write.mode("overwrite").parquet(
            str(REJECTED_ORDERS_PATH)
        )

        malformed_orders.write.mode("overwrite").parquet(
            str(MALFORMED_ORDERS_PATH)
        )

        report.write.mode("overwrite").parquet(
            str(RECONCILIATION_REPORT_PATH)
        )

        report.show(truncate=False)

        logger.info(
            "Reconciliation passed | input=%s | valid=%s | "
            "rejected=%s | malformed=%s",
            input_count,
            valid_count,
            rejected_count,
            malformed_count,
        )

        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()

        logger.info(
            "Pipeline completed successfully | run_id=%s | "
            "start_time=%s | end_time=%s | duration_seconds=%.2f",
            run_id,
            start_time.isoformat(),
            end_time.isoformat(),
            duration_seconds,
        )

    except Exception:
        logger.exception("Pipeline failed | run_id=%s", run_id)
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

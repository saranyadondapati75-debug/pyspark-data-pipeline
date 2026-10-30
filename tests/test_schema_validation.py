
import pytest
from pyspark.sql import SparkSession

from src.schema_validation import validate_input_schema


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("SchemaValidationTests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_valid_schema_passes(spark):
    customers = spark.createDataFrame(
        [("C001", "Anu", "Hyderabad")],
        ["customer_id", "customer_name", "city"]
    )

    orders = spark.createDataFrame(
        [("O001", "C001", "100", "pending", "2026-10-08T10:00:00")],
        ["order_id", "customer_id", "amount", "order_status", "order_timestamp"]
    )

    validate_input_schema(customers, orders)


def test_missing_order_column_fails(spark):
    customers = spark.createDataFrame(
        [("C001", "Anu", "Hyderabad")],
        ["customer_id", "customer_name", "city"]
    )

    orders = spark.createDataFrame(
        [("O001", "C001", "100", "pending")],
        ["order_id", "customer_id", "amount", "order_status"]
    )

    with pytest.raises(ValueError, match="order_timestamp"):
        validate_input_schema(customers, orders)


def test_missing_customer_column_fails(spark):
    customers = spark.createDataFrame(
        [("C001", "Anu")],
        ["customer_id", "customer_name"]
    )

    orders = spark.createDataFrame(
        [("O001", "C001", "100", "pending", "2026-10-08T10:00:00")],
        ["order_id", "customer_id", "amount", "order_status", "order_timestamp"]
    )

    with pytest.raises(ValueError, match="city"):
        validate_input_schema(customers, orders)

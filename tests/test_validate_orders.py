
import pytest
from pyspark.sql import SparkSession

from src.order_validation import validate_orders


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("OrderValidationTests")
        .master("local[2]")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def customers(spark):
    return spark.createDataFrame(
        [
            ("C001", "Anu", "Hyderabad"),
            ("C002", "Ravi", "Vijayawada"),
        ],
        ["customer_id", "customer_name", "city"],
    )


def make_order(spark, order_id="O001", customer_id="C001",
               amount="100.0", timestamp="2026-10-08T10:00:00"):
    return spark.createDataFrame(
        [(order_id, customer_id, amount, "COMPLETED", timestamp)],
        ["order_id", "customer_id", "amount",
         "order_status", "order_timestamp"],
    )


def test_valid_order_is_accepted(spark, customers):
    orders = make_order(spark)

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 1
    assert rejected.count() == 0


def test_unknown_customer_is_rejected(spark, customers):
    orders = make_order(spark, customer_id="C999")

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.select("reject_reason").first()[0] == "unknown_customer"


def test_negative_amount_is_rejected(spark, customers):
    orders = make_order(spark, amount="-10")

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.select("reject_reason").first()[0] == "non_positive_amount"


def test_invalid_amount_is_rejected(spark, customers):
    orders = make_order(spark, amount="not-a-number")

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.select("reject_reason").first()[0] == "invalid_amount"


def test_invalid_timestamp_is_rejected(spark, customers):
    orders = make_order(spark, timestamp="not-a-timestamp")

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.select("reject_reason").first()[0] == "invalid_timestamp"


def test_both_duplicate_order_ids_are_rejected(spark, customers):
    orders = spark.createDataFrame(
        [
            ("O001", "C001", "100", "COMPLETED", "2026-10-08T10:00:00"),
            ("O001", "C002", "200", "COMPLETED", "2026-10-08T11:00:00"),
        ],
        ["order_id", "customer_id", "amount",
         "order_status", "order_timestamp"],
    )

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.count() == 2
    assert all(
        row["reject_reason"] == "duplicate_order_id"
        for row in rejected.select("reject_reason").collect()
    )

def test_missing_customer_id_is_rejected(spark, customers):
    orders = make_order(spark, customer_id="")

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.select("reject_reason").first()[0] == "missing_customer_id"


def test_missing_order_id_is_rejected(spark, customers):
    orders = make_order(spark, order_id="")

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.select("reject_reason").first()[0] == "missing_order_id"


def test_empty_batch_returns_no_records(spark, customers):
    orders = spark.createDataFrame(
        [],
        "order_id string, customer_id string, amount string, "
        "order_status string, order_timestamp string",
    )

    valid, rejected = validate_orders(customers, orders)

    assert valid.count() == 0
    assert rejected.count() == 0


from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, to_timestamp, when, count
)


def validate_orders(
    customers: DataFrame,
    orders: DataFrame
) -> tuple[DataFrame, DataFrame]:

    # Normalize fields and convert expected types.
    orders = (
        orders
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn(
            "parsed_timestamp",
            to_timestamp(
                col("order_timestamp"),
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # Flag every occurrence of a duplicate order ID.
    duplicate_ids = (
        orders.groupBy("order_id")
        .agg(count("*").alias("id_count"))
        .filter(col("id_count") > 1)
        .select("order_id")
        .withColumn(
            "is_duplicate",
            col("order_id").isNotNull()
        )
    )

    orders = orders.join(
        duplicate_ids, on="order_id", how="left"
    )

    # Check customer references.
    customer_ids = customers.select(
        "customer_id"
    ).distinct()

    orders = orders.join(
        customer_ids.withColumn(
            "customer_exists",
            col("customer_id").isNotNull()
        ),
        on="customer_id",
        how="left"
    )

    # Assign a rejection reason.
    orders = orders.withColumn(
        "reject_reason",
        when(
            col("order_id").isNull() |
            (col("order_id") == ""),
            "missing_order_id"
        )
        .when(
            col("customer_id").isNull() |
            (col("customer_id") == ""),
            "missing_customer_id"
        )
        .when(
            col("amount").isNull(),
            "invalid_amount"
        )
        .when(
            col("amount") <= 0,
            "non_positive_amount"
        )
        .when(
            col("parsed_timestamp").isNull(),
            "invalid_timestamp"
        )
        .when(
            col("customer_exists").isNull(),
            "unknown_customer"
        )
        .when(
            col("is_duplicate") == True,
            "duplicate_order_id"
        )
    )

    valid_orders = orders.filter(
        col("reject_reason").isNull()
    )
    rejected_orders = orders.filter(
        col("reject_reason").isNotNull()
    )

    # Keep rejection reasons in the quarantine output.
    valid_orders = valid_orders.drop(
        "is_duplicate",
        "customer_exists",
        "reject_reason"
    )
    rejected_orders = rejected_orders.drop(
        "is_duplicate",
        "customer_exists"
    )

    return valid_orders, rejected_orders

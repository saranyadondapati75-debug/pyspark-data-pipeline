
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_timestamp, when, count
)

spark = (
    SparkSession.builder
    .appName("YottaFlexOrderValidation")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

customers = (
    spark.read.option("header", True)
    .csv("data/customers.csv")
)

orders = (
    spark.read.option("header", True)
    .csv("data/orders.csv")
)

# Convert columns to their expected types.
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

# Identify every order ID that occurs more than once.
duplicate_ids = (
    orders.groupBy("order_id")
    .agg(count("*").alias("id_count"))
    .filter(col("id_count") > 1)
    .select("order_id")
    .withColumn("is_duplicate", col("order_id").isNotNull())
)

orders = orders.join(duplicate_ids, on="order_id", how="left")

# Check that each customer ID exists.
customer_ids = customers.select("customer_id").distinct()

orders = orders.join(
    customer_ids.withColumn("customer_exists", col("customer_id").isNotNull()),
    on="customer_id",
    how="left"
)

# Give each rejected record a clear reason.
orders = orders.withColumn(
    "reject_reason",
    when(
        col("order_id").isNull() | (col("order_id") == ""),
        "missing_order_id"
    )
    .when(
        col("customer_id").isNull() | (col("customer_id") == ""),
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

valid_orders = orders.filter(col("reject_reason").isNull())
rejected_orders = orders.filter(col("reject_reason").isNotNull())

# Remove helper columns from the published data.
helper_columns = ["is_duplicate", "customer_exists"]
valid_orders = valid_orders.drop(*helper_columns, "reject_reason")
rejected_orders = rejected_orders.drop(*helper_columns, "customer_exists")

# Display and save the results.
print("Input count:", orders.count())
print("Valid count:", valid_orders.count())
print("Rejected count:", rejected_orders.count())

print("\nRejected records and reasons:")
rejected_orders.select("order_id", "customer_id", "amount", "reject_reason") \
    .show(20, truncate=False)

valid_orders.write.mode("overwrite").parquet("output/valid_orders")
rejected_orders.write.mode("overwrite").parquet("output/rejected_orders")

print("Validation finished.")

spark.stop()


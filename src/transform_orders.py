
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as spark_sum,
    count,
    broadcast,
)

from src.config import (
    CUSTOMERS_FILE,
    VALID_ORDERS_PATH,
    ENRICHED_ORDERS_PATH,
    CITY_SALES_REPORT_PATH,
)


def main():
    spark = (
        SparkSession.builder
        .appName("YottaFlexOrderTransformation")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        # Read validated orders and customer data.
        print("\nReading input datasets...")

        orders = spark.read.parquet(str(VALID_ORDERS_PATH))

        customers = spark.read.option(
            "header", True
        ).csv(str(CUSTOMERS_FILE))

        # -----------------------------------------
        # 1. Benchmark normal join
        # -----------------------------------------
        print("\n--- NORMAL JOIN BENCHMARK ---")

        normal_start = time.perf_counter()

        normal_enriched_orders = orders.join(
            customers,
            on="customer_id",
            how="inner"
        )

        normal_count = normal_enriched_orders.count()

        normal_join_time = time.perf_counter() - normal_start

        print(f"Normal join records: {normal_count}")
        print(f"Normal join time: {normal_join_time:.2f} seconds")

        # -----------------------------------------
        # 2. Benchmark optimized broadcast join
        # -----------------------------------------
        print("\n--- BROADCAST JOIN BENCHMARK ---")

        broadcast_start = time.perf_counter()

        enriched_orders = orders.join(
            broadcast(customers),
            on="customer_id",
            how="inner"
        )

        broadcast_count = enriched_orders.count()

        broadcast_join_time = (
            time.perf_counter() - broadcast_start
        )

        print(f"Broadcast join records: {broadcast_count}")
        print(
            f"Broadcast join time: "
            f"{broadcast_join_time:.2f} seconds"
        )

        # -----------------------------------------
        # 3. Compare join performance
        # -----------------------------------------
        print("\n--- JOIN PERFORMANCE COMPARISON ---")

        print(f"Normal join: {normal_join_time:.2f} seconds")
        print(f"Broadcast join: {broadcast_join_time:.2f} seconds")

        if normal_count != broadcast_count:
            raise ValueError(
                "Normal join and broadcast join produced "
                "different record counts."
            )

        if broadcast_join_time < normal_join_time:
            improvement = (
                (normal_join_time - broadcast_join_time)
                / normal_join_time
            ) * 100

            print(
                f"Broadcast join improvement: "
                f"{improvement:.2f}%"
            )
        else:
            print(
                "Broadcast join was not faster in this run."
            )

        # -----------------------------------------
        # 4. Display enriched orders
        # -----------------------------------------
        print("\n--- ENRICHED ORDERS ---")

        enriched_orders.select(
            "order_id",
            "customer_id",
            "customer_name",
            "city",
            "amount",
            "order_status"
        ).show(10, truncate=False)

        # -----------------------------------------
        # 5. Calculate city-wise sales
        # -----------------------------------------
        city_report = (
            enriched_orders
            .groupBy("city")
            .agg(
                spark_sum("amount").alias("total_sales"),
                count("order_id").alias("order_count")
            )
            .orderBy("city")
        )

        print("\n--- SALES REPORT BY CITY ---")
        city_report.show(truncate=False)

        # -----------------------------------------
        # 6. Save transformed datasets
        # -----------------------------------------
        print("\nSaving transformed datasets...")

        enriched_orders.write.mode("overwrite").parquet(
            str(ENRICHED_ORDERS_PATH)
        )

        city_report.write.mode("overwrite").parquet(
            str(CITY_SALES_REPORT_PATH)
        )

        print("\nTransformation completed successfully!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

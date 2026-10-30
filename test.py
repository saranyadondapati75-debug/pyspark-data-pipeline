from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ParquetTest") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame(
    [(1, "Anu"), (2, "Ravi")],
    ["id", "name"]
)

df.show()

df.write.mode("overwrite").parquet("output/test_parquet")

print("Parquet test completed!")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("MalformedCSVTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

orders = (
    spark.read
    .schema(schema)
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("unescapedQuoteHandling", "STOP_AT_DELIMITER")
    .csv("data/malformed_test.csv")
)

orders.select("order_id", "_corrupt_record").show(truncate=False)

print("Total rows:", orders.count())
print(
    "Malformed rows:",
    orders.filter(col("_corrupt_record").isNotNull()).count()
)

spark.stop()

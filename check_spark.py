
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("YottaFlexDataPipeline")
    .master("local[*]")
    .getOrCreate()
)

print("PySpark is working!")
print("Spark version:", spark.version)

spark.stop()

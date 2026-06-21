from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RawToBronze")
    .getOrCreate()
)

df = spark.read.csv(
    "/opt/airflow/data/raw/online_retail.csv",
    header=True,
    inferSchema=True,
    sep=","
)

df.write.mode("overwrite").parquet(
    "/opt/airflow/data/bronze/online_retail_bronze"
)

print(
    f"Bronze write complete. "
    f"Row count: {df.count()}"
)

spark.stop()
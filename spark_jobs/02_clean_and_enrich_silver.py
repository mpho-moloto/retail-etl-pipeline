from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    round as spark_round,
    to_timestamp,
    coalesce,
    month,
    year,
)

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/airflow/data/bronze/online_retail_bronze"
)

df = df.withColumn(
    "UnitPrice",
    col("UnitPrice").cast("string")
)

df_clean = (
    df
    .filter(col("CustomerID").isNotNull())
    .filter(col("Quantity") > 0)
    .dropDuplicates()
)

df_clean = df_clean.withColumn(
    "UnitPrice",
    regexp_replace(col("UnitPrice"), ",", ".")
    .cast("double")
)

df_enriched = (
    df_clean
    .withColumn(
        "TotalPrice",
        spark_round(
            col("Quantity") * col("UnitPrice"),
            2
        )
    )
    .withColumn(
        "InvoiceDate",
        coalesce(
            to_timestamp(
                col("InvoiceDate"),
                "yyyy/MM/dd HH:mm:ss"
            ),
            to_timestamp(
                col("InvoiceDate"),
                "yyyy/MM/dd HH:mm"
            )
        )
    )
    .withColumn(
        "InvoiceMonth",
        month(col("InvoiceDate"))
    )
    .withColumn(
        "InvoiceYear",
        year(col("InvoiceDate"))
    )
)

df_enriched.write.mode("overwrite").parquet(
    "/opt/airflow/data/silver/retail_silver"
)

print(
    f"Silver write complete. "
    f"Row count: {df_enriched.count()}"
)

spark.stop()
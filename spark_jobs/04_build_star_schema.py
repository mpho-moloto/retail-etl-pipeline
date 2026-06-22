from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    dayofmonth,
    month,
    year,
    monotonically_increasing_id,
)

spark = (
    SparkSession.builder
    .appName("BuildStarSchema")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/airflow/data/silver/retail_silver"
)

dim_customers = (
    df
    .select("CustomerID", "Country")
    .dropDuplicates(["CustomerID"])
)

dim_products = (
    df
    .select("StockCode", "Description")
    .dropDuplicates(["StockCode"])
)

dim_date = (
    df
    .select(
        to_date(col("InvoiceDate")).alias("Date")
    )
    .dropDuplicates(["Date"])
    .withColumn(
        "DateKey",
        monotonically_increasing_id()
    )
    .withColumn(
        "Day",
        dayofmonth(col("Date"))
    )
    .withColumn(
        "Month",
        month(col("Date"))
    )
    .withColumn(
        "Year",
        year(col("Date"))
    )
)

df_with_date = df.withColumn(
    "Date",
    to_date(col("InvoiceDate"))
)

fact_sales = (
    df_with_date
    .join(
        dim_date.select("Date", "DateKey"),
        on="Date",
        how="left"
    )
    .select(
        "InvoiceNo",
        "CustomerID",
        "StockCode",
        "DateKey",
        "Quantity",
        "UnitPrice",
        "TotalPrice",
        month(col("Date")).alias("InvoiceMonth"),
        year(col("Date")).alias("InvoiceYear"),
    )
)

pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_props = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

dim_customers.write.jdbc(
    pg_url,
    "dim_customers",
    mode="overwrite",
    properties=pg_props
)

dim_products.write.jdbc(
    pg_url,
    "dim_products",
    mode="overwrite",
    properties=pg_props
)

dim_date.write.jdbc(
    pg_url,
    "dim_date",
    mode="overwrite",
    properties=pg_props
)

fact_sales.write \
    .partitionBy("InvoiceYear", "InvoiceMonth") \
    .mode("overwrite") \
    .parquet(
        "/opt/airflow/data/silver/fact_sales_partitioned"
    )

fact_sales.write.jdbc(
    pg_url,
    "fact_sales",
    mode="overwrite",
    properties=pg_props
)

print(
    "Star schema built: "
    "dim_customers, dim_products, "
    "dim_date, fact_sales"
)

spark.stop()
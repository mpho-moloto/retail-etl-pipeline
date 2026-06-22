from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

spark = (
    SparkSession.builder
    .appName("DataQualityChecks")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/airflow/data/silver/retail_silver"
)

errors = []

row_count = df.count()
if row_count == 0:
    errors.append(
        "Silver dataset is empty."
    )

null_customer_count = (
    df.filter(col("CustomerID").isNull())
    .count()
)
if null_customer_count > 0:
    errors.append(
        f"Found {null_customer_count} rows "
        f"with null CustomerID."
    )

negative_revenue_count = (
    df.filter(col("TotalPrice") < 0)
    .count()
)
if negative_revenue_count > 0:
    errors.append(
        f"Found {negative_revenue_count} rows "
        f"with negative TotalPrice."
    )

null_date_count = (
    df.filter(col("InvoiceDate").isNull())
    .count()
)
if null_date_count > 0:
    errors.append(
        f"Found {null_date_count} rows "
        f"with null InvoiceDate."
    )

if errors:
    print("DATA QUALITY CHECK FAILED:")
    for e in errors:
        print(f" - {e}")
    sys.exit(1)
else:
    print(
        f"DATA QUALITY CHECK PASSED. "
        f"Row count: {row_count}"
    )

spark.stop()
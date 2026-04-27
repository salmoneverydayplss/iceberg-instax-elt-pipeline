import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_dq_check(target_date):
    spark = SparkSession.builder \
        .appName(f"DQ_Check_{target_date}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .getOrCreate()

    # 1. Load data from Silver 
    df = spark.table("local.db.silver_sales").filter(col("Date") == target_date)
    
    row_count = df.count()
    print(f"Starting Data Quality Check for {target_date} (Total rows: {row_count})")

    if row_count == 0:
        print("No data found for this date. Skipping DQ.")
        return

    errors = []

    # Check 1: Null Check
    null_cols = ["Date", "Product_Name", "Total_Sales_THB"]
    for c in null_cols:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            errors.append(f"FAILED: Column '{c}' has {null_count} null values.")

    # Check 2: Value Range Check
    negative_sales = df.filter(col("Total_Sales_THB") < 0).count()
    if negative_sales > 0:
        errors.append(f"FAILED: Found {negative_sales} rows with negative sales amount.")

    # Check 3: Positive Quantity Check
    invalid_qty = df.filter(col("Quantity") <= 0).count()
    if invalid_qty > 0:
        errors.append(f"FAILED: Found {invalid_qty} rows with zero or negative quantity.")

    if errors:
        print("-" * 30)
        for error in errors:
            print(error)
        print("-" * 30)
        raise Exception("Data Quality Check Failed! Please check the logs.")
    else:
        print(" All Data Quality Checks Passed!")

    spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1]
    run_dq_check(target_date)
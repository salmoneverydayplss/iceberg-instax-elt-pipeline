import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col

def process_gold_stage(target_date):
    spark = SparkSession.builder \
        .appName(f"ELT_Gold_Stage_{target_date}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .getOrCreate()

    silver_df = spark.table("local.db.silver_sales").filter(col("Date") == target_date)

    daily_summary_df = silver_df.groupBy("Date").agg(
        sum("Total_Sales_THB").alias("Total_Revenue_THB"),
        sum("Quantity").alias("Total_Items_Sold"),
        count("Product_Name").alias("Total_Transactions")
    )


    table_name = "local.db.gold_daily_summary"
    
    if spark.catalog.tableExists(table_name):
        
        daily_summary_df.writeTo(table_name).append()
    else:
        
        daily_summary_df.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy("Date") \
            .create()

    print(f"Gold stage summary completed for date: {target_date}")
    spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2022-05-01"
    process_gold_stage(target_date)
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, lit

def process_silver_stage(target_date):
    spark = SparkSession.builder \
        .appName(f"ELT_Silver_Stage_{target_date}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .getOrCreate()

    # 1. Extract
    daily_df = spark.table("local.db.bronze_sales").filter(col("Tanggal") == target_date)

    # 2. Transform
    exchange_rate = 0.0022
    
    silver_df = daily_df \
        .withColumnRenamed("Tanggal", "Date") \
        .withColumnRenamed("Tahun", "Year") \
        .withColumnRenamed("Bulan", "Month") \
        .withColumnRenamed("Hari", "Day") \
        .withColumnRenamed("Kategori", "Category") \
        .withColumnRenamed("Nama_Produk", "Product_Name") \
        .withColumnRenamed("Lokasi_Toko", "Store_Location") \
        .withColumnRenamed("Metode_Bayar", "Payment_Method") \
        .withColumnRenamed("Qty", "Quantity") \
        .withColumn("Category", when(col("Category") == "Kamera", "Camera")
                               .when(col("Category") == "Aksesoris", "Accessories")
                               .otherwise(col("Category"))) \
        .withColumn("Payment_Method", when(col("Payment_Method") == "Kartu Kredit", "Credit Card")
                                     .when(col("Payment_Method") == "Tunai", "Cash")
                                     .otherwise(col("Payment_Method"))) \
        .withColumn("Unit_Price_THB", round(col("Harga_Satuan") * exchange_rate, 2)) \
        .withColumn("Discount_THB", round(col("Diskon_IDR") * exchange_rate, 2)) \
        .withColumn("Total_Sales_THB", round(col("Total_Penjualan") * exchange_rate, 2)) \
        .drop("Harga_Satuan", "Diskon_IDR", "Total_Penjualan")

    # 3. Load
    table_name = "local.db.silver_sales"
    
    if spark.catalog.tableExists(table_name):
        silver_df.writeTo(table_name).append()
    else:
        silver_df.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy("Date") \
            .create()

    print(f"Silver stage processing completed for date: {target_date}")
    spark.stop()

if __name__ == "__main__":
   
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2022-05-01"
    process_silver_stage(target_date)
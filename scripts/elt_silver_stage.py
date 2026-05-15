import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, lit, udf
from pyspark.sql.types import StringType
from deep_translator import GoogleTranslator

# --- Exchange rate Real-time Function ---
def get_live_exchange_rate():
    try:
        #USE Frankfurter API (Free and not use API Key)
        url = "https://api.frankfurter.app/latest?from=IDR&to=THB"
        response = requests.get(url)
        data = response.json()
        return data['rates']['THB']
    except Exception as e:
        return 0.0022

def translate_text(text):
    if text is None or text == "":
        return text
    try:
        # Translate (id) --> (en)
        return GoogleTranslator(source='id', target='en').translate(text)
    except:
        return text

translate_udf = udf(translate_text, StringType())

from pyspark.sql.functions import to_date, lit

def process_silver_stage(target_date):

    spark = SparkSession.builder \
        .appName(f"ELT_Silver_Stage_{target_date}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    # 1. Extract
    daily_df = spark.table("local.db.bronze_sales").filter(col("Tanggal") == target_date)

    if daily_df.count() == 0:
        print(f"No data for date {target_date}")
        return

    # 2. Transform
    # Pull Real-time
    live_rate = get_live_exchange_rate()
    print(f"Current Exchange Rate (IDR to THB): {live_rate}")
    
    unique_categories = daily_df.select("Kategori").distinct()
    translated_cat = unique_categories.withColumn("Category_EN", translate_udf(col("Kategori"))).distinct()
    
    unique_methods = daily_df.select("Metode_Bayar").distinct()
    translated_methods = unique_methods.withColumn("Payment_EN", translate_udf(col("Metode_Bayar"))).distinct()

    #Join into main table
    silver_df = daily_df \
        .join(translated_cat, "Kategori", "left") \
        .join(translated_methods, "Metode_Bayar", "left") \
        .withColumnRenamed("Tanggal", "Date") \
        .withColumnRenamed("Tahun", "Year") \
        .withColumnRenamed("Bulan", "Month") \
        .withColumnRenamed("Hari", "Day") \
        .withColumnRenamed("Nama_Produk", "Product_Name") \
        .withColumnRenamed("Lokasi_Toko", "Store_Location") \
        .withColumnRenamed("Qty", "Quantity") \
        .withColumn("Category", col("Category_EN")) \
        .withColumn("Payment_Method", col("Payment_EN")) \
        .withColumn("Unit_Price_THB", round(col("Harga_Satuan") * live_rate, 2)) \
        .withColumn("Discount_THB", round(col("Diskon_IDR") * live_rate, 2)) \
        .withColumn("Total_Sales_THB", round(col("Total_Penjualan") * live_rate, 2)) \
        .select("Date", "Year", "Month", "Day", "Category", "Product_Name", 
                "Store_Location", "Payment_Method", "Quantity", 
                "Unit_Price_THB", "Discount_THB", "Total_Sales_THB", 
                "ingestion_timestamp", "source_file")

    # 3. Load
    table_name = "local.db.silver_sales"
    import shutil
    import os

    # 1. ลบ Metadata ใน Catalog
    if spark.catalog.tableExists(table_name):
        print(f"--- 🧨 DROP TABLE: {table_name} ---")
        spark.sql(f"DROP TABLE {table_name}")
    
    # 2. ลบ Physical Files ในเครื่อง (เช็กให้ชัวร์ว่า Path ถูก)
    # ปกติ Iceberg Hadoop Catalog จะเก็บที่: warehouse/namespace/tablename
    warehouse_path = "/tmp/iceberg_warehouse/db/silver_sales"
    if os.path.exists(warehouse_path):
        print(f"--- 🗑️ PHYSICAL DELETE: {warehouse_path} ---")
        shutil.rmtree(warehouse_path)

    # 3. สร้างตารางใหม่แกะกล่อง
    print("--- 🆕 CREATING NEW TABLE WITH FRESH ROWS ---")
    silver_df.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .partitionedBy("Date") \
        .create()

    # 4. แสดงผลลัพธ์เพื่อยืนยันความสำเร็จ
    final_count = spark.table(table_name).count()
    print(f"--- ✅ SUCCESS: Current count in {table_name} is {final_count} rows ---")


    spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2022-05-01"
    process_silver_stage(target_date)
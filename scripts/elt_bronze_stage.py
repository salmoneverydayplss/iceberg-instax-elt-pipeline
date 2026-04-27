import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

def process_bronze_stage(target_date):
    spark = SparkSession.builder \
        .appName(f"ELT_Bronze_Stage_{target_date}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .getOrCreate()

    # 1. Extract: อ่านข้อมูลจากไฟล์ CSV
    # แก้ไขบรรทัดนี้ให้อ่าน Header และ Infer Schema
    raw_df = spark.read.csv("/opt/airflow/data/instax_sales_transaction_data.csv", header=True, inferSchema=True)
    # กรองเฉพาะวันที่ต้องการประมวลผล
    daily_df = raw_df.filter(col("Tanggal") == target_date)

    # 2. Add Metadata (Best Practice สำหรับชั้น Bronze)
    bronze_df = daily_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("instax_sales_transaction_data.csv"))

    # 3. Load: บันทึกลง Iceberg Table (Bronze Stage)
    # ใช้คอลัมน์ Tanggal เป็นตัว Partition
    table_name = "local.db.bronze_sales"
    
    if spark.catalog.tableExists(table_name):
        # ถ้ามีตารางอยู่แล้ว ให้ append ข้อมูลเพิ่มเข้าไป
        bronze_df.writeTo(table_name) \
            .append()
    else:
        # ถ้ายังไม่มีตาราง (รันครั้งแรก) ให้สร้างใหม่และตั้งค่า Partition
        bronze_df.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy("Tanggal") \
            .create()

    print(f"Bronze stage processing completed for date: {target_date}")
    spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2022-05-01"
    process_bronze_stage(target_date)
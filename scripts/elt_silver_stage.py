import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, lit

def process_silver_stage(target_date):
    # กำหนดค่า Iceberg Catalog ใน SparkSession (สมมติใช้ local catalog)
    spark = SparkSession.builder \
        .appName(f"ELT_Silver_Stage_{target_date}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .getOrCreate()

    # 1. Extract: อ่านข้อมูล Raw Data
    # ในการทำงานจริง อาจจะมีโฟลเดอร์ที่รับไฟล์รายวัน เช่น /raw_data/yyyy-mm-dd/
    daily_df = spark.table("local.db.bronze_sales").filter(col("Tanggal") == target_date)

    # 2. Transform: เปลี่ยนชื่อคอลัมน์ และ Data Mapping (ภาษาอินโดนีเซีย -> อังกฤษ)
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

    # 3. Load: บันทึกข้อมูลลง Iceberg Table (Silver Stage)
    # ใช้ append เพื่อเพิ่มข้อมูลรายวัน และ Iceberg จะสร้าง Snapshot ให้โดยอัตโนมัติ
    table_name = "local.db.silver_sales"
    
    if spark.catalog.tableExists(table_name):
        # ถ้ามีตารางอยู่แล้ว ให้ append ข้อมูลเพิ่ม
        silver_df.writeTo(table_name).append()
    else:
        # ถ้ายังไม่มีตาราง ให้สร้างใหม่
        silver_df.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy("Date") \
            .create()

    print(f"Silver stage processing completed for date: {target_date}")
    spark.stop()

if __name__ == "__main__":
    # รับค่าวันที่จาก Argument ที่ Airflow ส่งมาให้
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2022-05-01"
    process_silver_stage(target_date)
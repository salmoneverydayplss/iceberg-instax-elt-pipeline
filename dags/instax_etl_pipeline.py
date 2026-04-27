from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default Arguments สำหรับ DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 1), # เริ่มตามวันที่ใน dataset
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'instax_sales_elt_iceberg',
    default_args=default_args,
    description='End-to-End ELT Pipeline for Instax Sales using Iceberg',
    schedule_interval='@daily', 
    catchup=True, # เก็บตกข้อมูลย้อนหลัง (สำคัญสำหรับการทำ historical snapshots)
) as dag:

    # 1. Stage Bronze: Extract CSV and Load to Iceberg Raw Table
    extract_to_bronze = SparkSubmitOperator(
        task_id='ingest_csv_to_bronze',
        application='scripts/bronze_ingestion.py',
        conn_id='spark_default',
        application_args=["{{ ds }}"], # ส่งวันที่ของ data ไปให้ spark script
    )

    # 2. Stage Silver: Transform (Translate Language & Currency Conversion)
    transform_to_silver = SparkSubmitOperator(
        task_id='transform_bronze_to_silver',
        application='scripts/silver_transformation.py',
        conn_id='spark_default',
        application_args=["{{ ds }}"],
    )

    # 3. Stage Gold: Daily Aggregate for Dashboard
    aggregate_to_gold = SparkSubmitOperator(
        task_id='aggregate_silver_to_gold',
        application='scripts/gold_aggregation.py',
        conn_id='spark_default',
        application_args=["{{ ds }}"],
    )

    # กำหนดลำดับการทำงาน (Dependency)
    extract_to_bronze >> transform_to_silver >> aggregate_to_gold
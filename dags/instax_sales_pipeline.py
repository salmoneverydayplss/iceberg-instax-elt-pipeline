from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.param import Param 
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'instax_end_to_end_elt',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2022, 5, 1),
    catchup=False,
    params={
        "target_date": Param(
            default="2022-05-01", 
            type="string", 
            format="date",
            description=" Specify the date you want to process (this dataset contains data only from the date range 2022-05-01 to 2025-05-01)"
        ),
    },
    tags=['sales', 'iceberg'],
) as dag:

    run_bronze_stage = BashOperator(
        task_id='load_to_bronze_iceberg',
        bash_command='spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 /opt/airflow/scripts/elt_bronze_stage.py {{ params.target_date }}',
    )

    run_silver_stage = BashOperator(
        task_id='transform_to_silver_iceberg',
        bash_command='spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 /opt/airflow/scripts/elt_silver_stage.py {{ params.target_date }}',
    )

    dq_check = BashOperator(
        task_id='data_quality_check_silver',
        bash_command='spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 /opt/airflow/scripts/data_quality_check.py {{ params.target_date }}',
    )

    run_gold_stage = BashOperator(
        task_id='aggregate_to_gold_iceberg',
        bash_command='spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 /opt/airflow/scripts/elt_gold_stage.py {{ params.target_date }}',
    )

    run_bronze_stage >> run_silver_stage >> dq_check >> run_gold_stage
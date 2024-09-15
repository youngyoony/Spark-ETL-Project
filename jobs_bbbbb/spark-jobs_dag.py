from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
    # "on_failure_callback": ,
}

dag = DAG(
    "spark_data_processing_pipeline",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # Runs at 1:00 AM every day
    catchup=False,
    tags=['data-processing', 'spark']
)

process_data = BashOperator(
    task_id='process_data',
    bash_command="bash /opt/airflow/jobs/spark-submit.sh /opt/airflow/jobs/main.py",
    dag=dag
)

# Task dependencies
process_data
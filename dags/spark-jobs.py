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
    'data_pipeline_api',
    default_args=default_args,
    max_active_runs=1, 
    schedule_interval="30 0 * * *", 
    catchup=False, 
    tags=['data']
)

# Python command to run fetch_data.py
fetch_data = BashOperator(
    task_id='fetch_data',
    bash_command='bash /opt/airflow/jobs/spark-submit.sh /opt/airflow/jobs/fetch_data.py ',
    dag=dag
)

# Python command to run process_data.py
process_data = BashOperator(
    task_id='process_data',
    bash_command='bash /opt/airflow/jobs/spark-submit.sh /opt/airflow/jobs/process_data.py ',
    dag=dag
)

# Python command to run analyze_data.py
analyze_data = BashOperator(
    task_id='analyze_data',
    bash_command='bash /opt/airflow/jobs/spark-submit.sh /opt/airflow/jobs/analyze_data.py ',
    dag=dag
)

# fetch_data >> process_data >> analyze_data
fetch_data
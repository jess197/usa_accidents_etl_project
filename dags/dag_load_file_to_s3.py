from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.load_csv_file_to_s3 import upload_csv_file_to_s3

default_args = {
    "owner": "Jess",
    "retries": 1,
    "retry_delay": 0 
}

def load_us_accident_data():
     file_path = '/opt/airflow/dags/files/US_Accidents_March23.csv'
     upload_csv_file_to_s3(file_path)

with DAG(
    dag_id="dag_load_file_to_s3", 
    start_date=datetime(2023, 6, 18),
    schedule_interval="@daily",
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['project', 'etl', 's3']
) as dag:
    
    load_us_accident_data_task = PythonOperator(
        task_id='load_us_accident_data_task',
        python_callable=load_us_accident_data
    )

    load_us_accident_data_task



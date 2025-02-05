import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from tasks.extract_api import extract_api
from tasks.extract_db import extract_db
from tasks.create_bucket import create_bucket
from tasks.process_raw_to_silver import process_raw_to_silver

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

TELEGRAM_API_URL = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

def send_message(task_id, status):
    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)
    day = str(datetime.now().day).zfill(2)
    hour = str(datetime.now().hour).zfill(2)
    minute = str(datetime.now().minute).zfill(2)
    message = f"Task {task_id} {status} at {hour}:{minute} on {month}/{day}/{year}."
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    try:
        response = requests.post(TELEGRAM_API_URL, data=payload)
        if response.status_code == 200:
            print(f"Message sent successfully: {message}")
        else:
            print(f"Failed to send message: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error sending message: {e}")

def task_success_callback(context):
    task_id = context['task_instance'].task_id
    send_message(task_id, 'succeeded')

def task_failure_callback(context):
    task_id = context['task_instance'].task_id
    send_message(task_id, 'failed')

default_args = {
    'owner': 'trkiet',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': task_success_callback,
    'on_failure_callback': task_failure_callback 
}

with DAG (
    dag_id = 'pipeline',
    default_args=default_args,
    description = 'Build an ELT pipeline!',
    # start_date = datetime(2024, 11, 20),
    start_date=days_ago(0),
    schedule_interval = '@monthly',
    catchup=False,
) as dag:
    create_bucket_task = PythonOperator(
        task_id="create_bucket",
        python_callable=create_bucket,
        provide_context=True
    )

    extract_api_task = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api,
        provide_context=True
    )

    extract_db_task = PythonOperator(
        task_id="extract_db",
        python_callable=extract_db,
        provide_context=True
    )

    process_raw_to_silver_task = PythonOperator(
        task_id="process_raw_to_silver",
        python_callable=process_raw_to_silver,
        provide_context=True
    )

    process_silver_to_golden_task = DockerOperator(
        task_id='process_silver_to_golden',
        image='kiettna/airflow-spark-job',  # Spark Docker image
        container_name="process_silver_to_golden",
        command="python process_silver_to_golden.py",
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',  # Docker socket
        network_mode='nyc-taxi-prediction-pipeline_default',  # Or the network of your Airflow setup
    )

    load_to_warehouse_task = DockerOperator(
        task_id='load_to_data_warehouse',
        image='kiettna/airflow-spark-job',  # Spark Docker image
        container_name="load_to_data_warehouse",
        command="python load_to_warehouse.py",
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',  # Docker socket
        network_mode='nyc-taxi-prediction-pipeline_default',  # Or the network of your Airflow setup
    )

    create_bucket_task >> [extract_api_task, extract_db_task] >> process_raw_to_silver_task >> [process_silver_to_golden_task, load_to_warehouse_task]
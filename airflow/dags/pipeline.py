import os
import requests
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from tasks.ingest2local import ingest_to_local
from tasks.covert2delta import convert_to_delta
from tasks.load2lake import load_to_data_lake

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

TELEGRAM_API_URL = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

def send_message(task_id, status):
    message = f"Task {task_id} has {status}."
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
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'on_success_callback': task_success_callback,
    'on_failure_callback': task_failure_callback 
}

with DAG (
    dag_id = 'pipeline',
    default_args=default_args,
    description = 'Build an ELT pipeline!',
    # start_date = datetime(2024, 11, 20),
    start_date=days_ago(0),
    schedule_interval = '@daily',
    catchup=False,
) as dag:
    ingest2local_task = PythonOperator(
        task_id="ingest_to_local",
        python_callable=ingest_to_local,
        provide_context=True
    )

    convert2delta_task = PythonOperator(
        task_id="convert_to_delta",
        python_callable=convert_to_delta,
        provide_context=True
    )

    load2lake_task = PythonOperator(
        task_id="load_to_data_lake",
        python_callable=load_to_data_lake,
        provide_context=True
    )

    load2warehouse_task = DockerOperator(
        task_id='load_to_data_warehouse',
        image='kiettna/airflow-spark-job',  # Spark Docker image
        container_name="airflow-spark-job",
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',  # Docker socket
        network_mode='nyc-taxi-prediction-pipeline_default',  # Or the network of your Airflow setup
    )

    ingest2local_task >> convert2delta_task >> load2lake_task >> load2warehouse_task
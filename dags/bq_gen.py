from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_bigquery = DAG(
    'generate_bigquery_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Executa diariamente
)

def generate_bigquery_data():
    client = bigquery.Client()
    dataset_id = 'your_dataset'
    table_id = 'your_table'

    data = {'value': random.randint(1, 100)}
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    client.insert_rows_json(table_ref, [data])

bigquery_task = PythonOperator(
    task_id='generate_bigquery_data',
    python_callable=generate_bigquery_data,
    dag=dag_bigquery,
)


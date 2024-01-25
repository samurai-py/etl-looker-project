from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_mongo = DAG(
    'generate_mongo_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Executa diariamente
)

def generate_mongo_data():
    client = MongoClient('mongodb://your_mongo_uri')
    db = client['your_database']
    collection = db['your_collection']

    data = {'value': random.randint(1, 100)}
    collection.insert_one(data)

mongo_task = PythonOperator(
    task_id='generate_mongo_data',
    python_callable=generate_mongo_data,
    dag=dag_mongo,
)


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_excel = DAG(
    'generate_excel_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Executa diariamente
)

def generate_excel_data():
    data = {'value': random.randint(1, 100)}
    df = pd.DataFrame([data])
    df.to_excel('your_file.xlsx', index=False)

excel_task = PythonOperator(
    task_id='generate_excel_data',
    python_callable=generate_excel_data,
    dag=dag_excel,
)


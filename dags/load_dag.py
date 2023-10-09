from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import dag, task
from lib import ConnectionBuilder
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 9),
}

dag = DAG(
    dag_id="load_dag",
    default_args=default_args,
    start_date=datetime(2023, 10, 9),
    schedule_interval=None,
    catchup=False,
)

def load_dag():
    # Создаем подключение к базе dwh.
    pg_connect = ConnectionBuilder.pg_conn("postgres_db_conn")

    start_task = DummyOperator(task_id="start")

    print_csv_files = PythonOperator(
        task_id='print_csv_files',
        application="/scripts/print.py",
        conn_id="pg_connect",
#        application_args=[
#            "/user/kirillzhul/data/analytics/"
#        ]
)

    end_task = DummyOperator(task_id="end")

    start_task >> print_csv_files >> end_task


load_dag = load_dag()  

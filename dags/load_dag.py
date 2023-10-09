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

start_task = DummyOperator(task_id="start")

load_date = SparkSubmitOperator(
    task_id="load_date",
    dag=dag,
    application="/lessons/users_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/kirillzhul/data/geo/events/",
        "/user/kirillzhul/geo_2.csv",
        "/user/kirillzhul/data/analytics/",
    ]    
)

end_task = DummyOperator(task_id="end")

start_task >> load_date >> end_task

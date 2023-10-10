from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from contextlib import contextmanager
from typing import Generator
import psycopg2
from airflow.hooks.base import BaseHook
from configparser import ConfigParser


def connect():
    conn = psycopg2.connect(dbname="project_db", host="95.143.191.48", user="project_user", password="project_password", port="5433")
    cur = conn.cursor()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

dag = DAG(
    dag_id="manager_dag",
    start_date=datetime.datetime(2023, 11, 5),
    schedule="@daily",
    catchup=False
)

start_task = DummyOperator(task_id="start")

def manager_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = connect()
  
    @task(task_id="load_managers")
    def load_managers():
        managers_loader = ManagerLoader(dwh_pg_connect, log)
        managers_loader.load_managers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные tasks.
    managers_load = load_managers()       

    managers_load
    
end_task = DummyOperator(task_id="end")

start_task >> managers_load >> end_task
    
manager_dag = manager_dag()  

import pendulum
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
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 9, 1),
    catchup=False,
    tags=["PET-Project", "dds"],
    is_paused_upon_creation=False
)

dwh_pg_connect = connect()

@task(task_id="load_managers")
def load_managers():
    managers_loader = ManagerLoader(dwh_pg_connect, log)
    managers_loader.load_managers()  # Вызываем функцию, которая перельет данные.
    
# Инициализируем объявленные tasks.
managers_load = load_managers() 

managers_load

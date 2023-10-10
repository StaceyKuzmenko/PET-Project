from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from pg_connect.py import ConnectionBuilder

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

def manager_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("postgres_db_conn")

    start_task = DummyOperator(task_id="start")
   
    @task(task_id="load_managers")
    def load_managers():
        managers_loader = ManagerLoader(dwh_pg_connect, log)
        managers_loader.load_managers()  # Вызываем функцию, которая перельет данные.

    end_task = DummyOperator(task_id="end")


    # Инициализируем объявленные tasks.
    managers_load = load_managers()
    

    # Далее задаем последовательность выполнения tasks.
    start_task >> managers_load >> end_task


manager_dag = manager_dag()  

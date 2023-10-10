from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from contextlib import contextmanager
from typing import Generator
import psycopg2
from airflow.hooks.base import BaseHook
from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Bagian {0} tidak ditemukan didalam {1} file'.format(section, filename))

    return db

def connect():
    """Koneksi ke PostgreSQL Database server"""
    conn = None
    try:
        params = config()
        print('Menghubungkan ke PostgreSQL database...')
        conn = psycopg2.connect(dbname="project_db", host="95.143.191.48", user="project_user", password="project_password", port="5433")

        cur = conn.cursor()

        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        db_version = cur.fetchone()
        print(db_version)
       
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Koneksi Database telah ditutup.')


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

def manager_dag():
    # Создаем подключение к базе dwh.
#    dwh_pg_connect = ConnectionBuilder.pg_conn("project_db")
    
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

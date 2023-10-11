from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import psycopg2
import json
from wsgiref import headers
import requests

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup

import pendulum
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from typing import Generator
import psycopg2
from airflow.hooks.base import BaseHook
from configparser import ConfigParser
from contextlib import contextmanager

### POSTGRESQL settings ###
# set postgresql connection from basehook
# all of these connections should be in Airflow as connectors

# pg_conn_1 = PostgresHook.get_connection('postgres_db_conn')

# init connection
# Connect to your local postgres DB (Docker)

DB_NAME = "project_db"
DB_USER = "project_user"
DB_PASS = "project_password"
DB_HOST = "95.143.191.48"
DB_PORT = "5433"

conn_1 = psycopg2.connect(
    database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)

# load data from STG
# paste data to DDS local connection
# MANAGERS TABLE
def load_managers_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "managers"

    # load to local to DB (managers)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    SELECT manager
    FROM stg.old_sales(manager)
    INSERT INTO dds.managers(manager)
    """

    cur_1.execute(postgres_insert_query)
    conn_1.commit()
    conn_1.close()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

dag = DAG(
    dag_id="manager_dag1",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 9, 1),
    catchup=False,
    tags=["PET-Project", "dds"],
    is_paused_upon_creation=False,
)

# create DAG logic (sequence/order)
t1 = DummyOperator(task_id="start")
t21 = PythonOperator(task_id="managers", python_callable=load_managers_to_dds, dag=dag)
#   t22 = PythonOperator(task_id="couriers", python_callable=load_paste_data_couriers, dag=dag)
#   t23 = PythonOperator(task_id="timestamps", python_callable=load_paste_data_timestamps, dag=dag)
#   t24 = PythonOperator(task_id="orders", python_callable=load_paste_data_orders, dag=dag)
#   t25 = PythonOperator(task_id="deliveries", python_callable=load_paste_data_deliveries, dag=dag)
t4 = DummyOperator(task_id="end")

t1 >> t21 >> t4

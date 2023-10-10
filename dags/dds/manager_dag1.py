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

### POSTGRESQL settings ###
# set postgresql connection from basehook
# all of these connections should be in Airflow as connectors

PG_WAREHOUSE_CONNECTION = {
    "host": "95.143.191.48",
    "user": "project_user",
    "password": "project_password",
    "port": 5433,
    "dbname": "project_db"
}

pg_conn_1 = PostgresHook.get_connection(PG_WAREHOUSE_CONNECTION)

# init connection
# Connect to your local postgres DB (Docker)
conn_1 = psycopg2.connect(
    f"""
    host='{pg_conn_1.host}'
    port='{pg_conn_1.port}'
    dbname='{pg_conn_1.dbname}' 
    user='{pg_conn_1.user}' 
    password='{pg_conn_1.password}'
    """
    )   

# load data from STG
# paste data to DDS local connection
# MANAGERS TABLE
def load_managers_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'managers'
    
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
    is_paused_upon_creation=False
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

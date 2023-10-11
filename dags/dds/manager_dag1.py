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
#import psycopg2
import psycopg
from airflow.hooks.base import BaseHook
from configparser import ConfigParser
from contextlib import contextmanager

class PgConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)

    def client(self):
        return psycopg.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = psycopg.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class ConnectionBuilder:

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg

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

#pg_conn_1 = PostgresHook.get_connection('postgres_db_conn')

# init connection
# Connect to your local postgres DB (Docker)
#conn_1 = psycopg2.connect(
#    f"""
#    host='95.143.191.48'
#    port='5433'
#    dbname='project_db' 
#    user='project_user' 
#    password='project_password'
#    """
#    )  

dwh_pg_connect = PgConnect(
    host=PG_WAREHOUSE_CONNECTION["host"],
    port=PG_WAREHOUSE_CONNECTION["port"],
    db_name=PG_WAREHOUSE_CONNECTION["database"],
    user=PG_WAREHOUSE_CONNECTION["user"],
    pw=PG_WAREHOUSE_CONNECTION["password"],
    sslmode="require" if PG_WAREHOUSE_CONNECTION["ssl"] else "disable"
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

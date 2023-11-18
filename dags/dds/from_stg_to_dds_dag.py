import pendulum
import time
from datetime import datetime, timedelta
from wsgiref import headers
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

### POSTGRESQL settings ###
# init connection
# Connect to your local postgres DB (Docker)

conn = BaseHook.get_connection('postgres_local')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 16),
}

execution_times = ["15 4", "15 9", "15 13"] # <<< this is in UTC (in UTC +3 07:15; 12:15; 16:15)

with DAG(
        'from_stg_to_dds_dag',                  
        default_args=default_args,         
        schedule_interval='15 4,9,13 * * *',  
        start_date=datetime(2023, 10, 16),  
        catchup=False,                     
        tags=['Pet-Project', 'stg', 'dds'],
) as dag:

    # create DAG logic (sequence/order)

    def get_sql_file_path(script_name):
        sql_script_directory = "sql"
        return f"{sql_script_directory}/{script_name}.sql"
	
    t1 = DummyOperator(task_id="start")
    t11 = PostgresOperator(task_id="managers", postgres_conn_id="postgres_local", sql=get_sql_file_path("dds_load_managers_to_dds"), autocommit=True, dag=dag)
    t12 = PostgresOperator(task_id="clients", postgres_conn_id="postgres_local", sql=get_sql_file_path("dds_load_clients_to_dds"), autocommit=True, dag=dag)
    t13 = PythonOperator(task_id="orders_realizations", postgres_conn_id="postgres_local", sql=get_sql_file_path("dds_load_orders_realization_to_dds"), autocommit=True, dag=dag)
    t2 = DummyOperator(task_id="end")

    '''for time in execution_times:
    dag.schedule_interval = f'{time} * * *'
    '''
	
    t1 >> t11 >> t12 >> t13 >> t2

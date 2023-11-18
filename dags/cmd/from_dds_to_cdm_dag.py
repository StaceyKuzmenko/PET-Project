import pendulum
import time
import psycopg2
from datetime import datetime, timedelta
from wsgiref import headers
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import SqlFileOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

### POSTGRESQL settings ###
# init connection
# Connect to your local postgres DB (Docker)

conn = BaseHook.get_connection('postgres_local')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 16),
}

execution_times = ["20 4", "20 9", "20 13"] # <<< this is in UTC (in UTC +3 07:20; 12:20; 16:20)

with DAG(
        'from_dds_to_cdm_dag',                  
        default_args=default_args,         
        schedule_interval='20 4,9,13 * * *',
        start_date=datetime(2023, 10, 16),  
        catchup=False,                     
        tags=['Pet-Project', 'dds', 'cdm'],
) as dag:

    # create DAG logic (sequence/order)
    
    def get_sql_file_path(script_name):
        sql_script_directory = "dags/sql"
        return f"{sql_script_directory}/{script_name}.sql"
    
    t10 = SqlFileOperator(task_id="all_months_to", postgres_conn_id="postgres_local", sql=get_sql_file_path("cdm_load_all_months_aggregated_sales"), autocommit=True, dag=dag,)
    t11 = SqlFileOperator(task_id="brands_to", postgres_conn_id="postgres_local", sql=get_sql_file_path("cdm_load_monthly_sales_by_brands"), autocommit=True, dag=dag,)
    t12 = SqlFileOperator(task_id="channels_to", postgres_conn_id="postgres_local", sql=get_sql_file_path("cdm_load_monthly_sales_by_sales_channels"), autocommit=True, dag=dag,)
    t13 = SqlFileOperator(task_id="aggregates_sales_to", postgres_conn_id="postgres_local", sql=get_sql_file_path("cdm_load_to_current_month_aggregated_sales"), autocommit=True, dag=dag,)
    t14 = SqlFileOperator(task_id="forecast_to", postgres_conn_id="postgres_local", sql=get_sql_file_path("cdm_load_to_monthly_sales_report"), autocommit=True, dag=dag,)

    t1 = DummyOperator(task_id="start")
    t2 = DummyOperator(task_id="end")

    '''for time in execution_times:
    dag.schedule_interval = f'{time} * * *'
    '''
    
    t1 >> t10 >> t11 >> t12 >> t13 >> t14 >> t2
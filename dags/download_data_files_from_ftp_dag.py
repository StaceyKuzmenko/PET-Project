import datetime
from library.ftp_download import get_files_from_ftp

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('ftp_conn')
folders = ('forecast', 'category', 'sales')

args = {
    "owner": "PET",
    'email': ['pet@pet.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}


with DAG(
    dag_id="download_files_from_ftp_to_local_folders",
    start_date=datetime.datetime(2023, 10, 5),
    description='Download 3 most recent files from data folders',
    schedule="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    download_files = PythonOperator(
        task_id='downloading_files',
        python_callable= get_files_from_ftp,
        op_kwargs={'folder_list': folders,
                   'host': conn.host,
                   'user': conn.login,
                   'passwd': conn.password
                   })
    
    drop_stg_tables = PostgresOperator(
        task_id="stg_dropping_tables",
        postgres_conn_id="postgres_local",
        sql="sql/stg_dropping_tables.sql",
        #params={"folder": folder},
)

(
    download_files >> drop_stg_tables
)
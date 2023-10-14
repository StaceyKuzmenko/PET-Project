import datetime
from library.ftp_download import get_files_from_ftp
from library.managing_files import find_the_latest_local_file_by_name

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
        sql="sql/stg_dropping_tables.sql"
)
    
    def create_loading_tasks(folder_name, latest_file):
        return PostgresOperator(
            task_id=f"stg_loading_table_{folder_name}",
            postgres_conn_id="postgres_local",
            sql=f"sql/stg_load_tables.sql",
            params={"folder": folder_name, "latest_file": latest_file})
    
    for folder in folders:
        latest_file = find_the_latest_local_file_by_name(folder)
        dynamic_task = create_loading_tasks(folder, latest_file)
        
(
    download_files >> drop_stg_tables >> dynamic_task
)

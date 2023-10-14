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
latest_file = find_the_latest_local_file_by_name(folders[0])

args = {
    "owner": "PET",
    'email': ['pet@pet.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}


with DAG(
    dag_id="upload_files_to_stg_layer",
    start_date=datetime.datetime(2023, 10, 5),
    description='Upload 3 files to STG-layer',
    schedule="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    clear_stg_tables = PostgresOperator(
        task_id="stg_clearing_tables",
        postgres_conn_id="postgres_local",
        sql="sql/stg_clearing_tables.sql"
)

    def create_loading_tasks(folder_name, latest_file):
        return PostgresOperator(
            task_id=f"stg_loading_table_{folder_name}",
            postgres_conn_id="postgres_local",
            sql=f"sql/stg_load_tables.sql",
            parameters={"folder": folder_name, "latest_file": latest_file})
    
    for folder in folders:
        latest_file = find_the_latest_local_file_by_name(folder)
        dynamic_task = create_loading_tasks(folder, latest_file)
        

(
    clear_stg_tables >> dynamic_task
)

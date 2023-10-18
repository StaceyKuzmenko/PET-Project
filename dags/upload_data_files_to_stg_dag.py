import datetime
from library.managing_files import find_the_latest_local_file_by_name

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

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
    dag_id="upload_files_to_stg_layer",
    start_date=datetime.datetime(2023, 10, 5),
    description='Upload 3 files to STG-layer',
    #schedule="@daily",
    schedule_interval = '10 4,9,13 * * *', # <<< this is in UTC (in UTC +3 07:05; 12:05; 16:05)
    catchup=False,
    tags=['stg'],
    max_active_runs=1
) as dag:

    clear_stg_tables = PostgresOperator(
        task_id="stg_clearing_tables",
        postgres_conn_id="postgres_local",
        sql="sql/stg_clearing_tables.sql"
)


    loading_sql_tasks = TaskGroup('load_files_to_stg')
    

    with loading_sql_tasks:
        for folder in folders:
            latest_file = find_the_latest_local_file_by_name(folder)
            PostgresOperator(
            task_id=f"stg_loading_table_{folder}",
            postgres_conn_id="postgres_local",
            sql=f"copy \"STG\".{folder} from '{latest_file}' with (format csv, delimiter \";\", header);"
            )


(
    clear_stg_tables >> loading_sql_tasks
)

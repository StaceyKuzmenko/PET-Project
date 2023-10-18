import datetime
from library.managing_files import find_the_latest_local_file_by_name

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

conn = BaseHook.get_connection('ftp_conn')
folders = ('old_sales',)
latest_file = find_the_latest_local_file_by_name('old_sales')

args = {
    "owner": "PET",
    'email': ['pet@pet.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


with DAG(
    dag_id="upload_old_sales_file_to_stg_layer",
    start_date=datetime.datetime(2023, 10, 5),
    description='Upload old_sales file to STG-layer',
    schedule=None,
    catchup=False,
    tags=['stg'],
    max_active_runs=1
) as dag:

    clear_stg_tables = PostgresOperator(
        task_id="stg_clearing_table",
        postgres_conn_id="postgres_local",
        sql="TRUNCATE TABLE \"STG\".old_sales;"
)
    
    upload_old_sales_to_stg = PostgresOperator(
    task_id=f"stg_loading_table_old_sales",
    postgres_conn_id="postgres_local",
    sql=f"copy \"STG\".old_sales from '{latest_file}' with (format csv, delimiter \";\", header);"
    )


(
    clear_stg_tables >> upload_old_sales_to_stg
)

import datetime
from library.ftp_download import get_files_from_ftp
from library.managing_files import find_the_latest_local_file_by_name

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('ftp_conn')
folders = ('old_sales',)

args = {
    "owner": "PET",
    'email': ['pet@pet.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}


with DAG(
    dag_id="download_old_sales_file_from_ftp",
    start_date=datetime.datetime(2023, 10, 5),
    description='Download old_sales file from ftp',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['ftp', '>>>', 'local']
) as dag:

    download_files = PythonOperator(
        task_id='downloading_files',
        python_callable= get_files_from_ftp,
        op_kwargs={'folder_list': folders,
                   'host': conn.host,
                   'user': conn.login,
                   'passwd': conn.password
                   })
    

(
    download_files
)

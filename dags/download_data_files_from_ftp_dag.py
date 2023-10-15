import datetime
from library.ftp_download import get_files_from_ftp

from airflow import DAG
from airflow.operators.python import PythonOperator
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

execution_times = ["5 4", "5 9", "5 13"] # <<< this is in UTC (in UTC +3 07:05; 12:05; 16:05)

with DAG(
    dag_id="download_files_from_ftp_to_local_folders",
    start_date=datetime.datetime(2023, 10, 5),
    description='Download 3 most recent files from data folders',
    #schedule="@daily",
    schedule_interval = None,
    catchup=False,
    tags=['ftp'],
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
    
    for time in execution_times:
        dag.schedule_interval = f'{time} * * *'
    
(
    download_files
)

import datetime
from library.ftp_download import get_files_from_ftp

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

#folders = ('forecast', 'category', 'sales', 'marketplaces')
folders = Variable.get('folders_list', deserialize_json=True)
print(folders)
conn = BaseHook.get_connection('ftp_conn')

args = {
    "owner": "PET",
    'email': ['pet@pet.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

with DAG(
    dag_id="download_files_from_ftp_to_local_folders",
    start_date=datetime.datetime(2023, 10, 14),
    description='Download 3 most recent files from data folders',
    schedule_interval = '5 4,9,13 * * *', # <<< this is in UTC (in UTC +3 07:05; 12:05; 16:05)
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
    
(
    download_files
)

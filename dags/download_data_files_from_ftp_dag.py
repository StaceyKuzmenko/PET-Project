import datetime
from lib.ftp_download import get_files_from_ftp

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

my_dag = DAG(
    dag_id="first_empty_dag",
    start_date=datetime.datetime(2023, 10, 5),
    schedule="@daily",
    catchup=False
)

EmptyOperator(task_id="task", dag=my_dag)

conn = BaseHook.get_connection('ftp_conn')

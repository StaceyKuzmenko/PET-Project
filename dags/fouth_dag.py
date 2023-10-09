import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

my_dag = DAG(
    dag_id="fouth_dag",
    start_date=datetime.datetime(2023, 11, 5),
    schedule="@daily",
    catchup=False
)

EmptyOperator(task_id="task", dag=my_dag)

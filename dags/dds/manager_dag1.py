import pendulum
import time
import psycopg2
from datetime import datetime, timedelta
from wsgiref import headers
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
#from airflow.operators.dummy import DummyOperator

### POSTGRESQL settings ###
# init connection
# Connect to your local postgres DB (Docker)

DB_NAME = "project_db"
DB_USER = "project_user"
DB_PASS = "project_password"
DB_HOST = "95.143.191.48"
DB_PORT = "5433"

conn_1 = psycopg2.connect(
    database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)

# load data from STG
# paste data to DDS local connection
# MANAGERS TABLE
def load_managers_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "managers"

    # load to local to DB (managers)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "DDS".managers(manager)
    SELECT manager  
    FROM "STG".old_sales
    """
    cur_1.execute(postgres_insert_query)
    conn_1.commit()
    conn_1.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
        'manager_dag1',                  
        default_args=default_args,         
        schedule_interval=None,  
        start_date=datetime(2023, 10, 10),  
        catchup=False,                     
        tags=['Pet-Project', 'dds'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t11 = PythonOperator(task_id="managers", python_callable=load_managers_to_dds, dag=dag)
    t2 = DummyOperator(task_id="end")

    t1 >> t11 >> t2

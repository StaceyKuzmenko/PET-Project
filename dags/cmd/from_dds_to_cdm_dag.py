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

# load data from DDS
# paste data to CDM local connection
def load_managers_to_monthly_sales_report():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_report"

    # load to local to DB (monthly_sales_report)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "CDM".monthly_sales_report(manager)
    SELECT manager  
    FROM "DDS".managers
    """
    cur_1.execute(postgres_insert_query)
    conn_1.commit()
    conn_1.close()

def load_clients_to_monthly_sales_report():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_report"

    # load to local to DB (monthly_sales_report)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "CDM".monthly_sales_report(client)
    SELECT client  
    FROM "DDS".clients
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

def load_product_category_to_monthly_sales_report():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_report"

    # load to local to DB (monthly_sales_report)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "CDM".monthly_sales_report(brand)
    SELECT brand  
    FROM "STG".product_category
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

def load_forecast_to_monthly_sales_report():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_report"

    # load to local to DB (monthly_sales_report)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "CDM".monthly_sales_report(general_plan, week_1, week_2, week_3, week_4, week_5)
    SELECT general_plan, week_1, week_2, week_3, week_4, week_5  
    FROM "STG".forecast
    """
    cur_1.execute(postgres_insert_query)  
    conn_1.commit()
    conn_1.close()











default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 16),
}

with DAG(
        'from_dds_to_cdm_dag',                  
        default_args=default_args,         
        schedule_interval="20 4,9,13 * * *",  
        start_date=datetime(2023, 10, 16),  
        catchup=False,                     
        tags=['Pet-Project', 'dds', 'cdm'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t11 = PythonOperator(task_id="managers_to", python_callable=load_managers_to_monthly_sales_report, dag=dag)
    t12 = PythonOperator(task_id="clients_to", python_callable=load_clients_to_monthly_sales_report, dag=dag)
    t13 = PythonOperator(task_id="product_category_to", python_callable=load_product_category_to_monthly_sales_report, dag=dag)
    t14 = PythonOperator(task_id="forecast_to", python_callable=load_forecast_to_monthly_sales_report, dag=dag)


    

    
    t2 = DummyOperator(task_id="end")

    t1 >> t11 >> t12 >> t13 >> t14 >> t2

conn_1.close()

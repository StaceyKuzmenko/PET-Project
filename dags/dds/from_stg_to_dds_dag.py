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
#DB_HOST = "95.143.191.48"
DB_HOST = "postgres_local":
DB_PORT = "5433"

conn_1 = psycopg2.connect(
    database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)

# load data from STG
# paste data to DDS local connection
def load_managers_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "managers"

    # load to local to DB (managers)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "DDS".managers(manager)
    SELECT distinct manager  
    FROM "STG".old_sales;
    """
    cur_1.execute(postgres_insert_query)
    conn_1.commit()
    conn_1.close()

def load_clients_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "clients"

    # load to local to DB (clients)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "DDS".clients(id_manager, client_id, client, sales_channel, region)
    SELECT m.id, os.client_id, os.client, os.sales_channel, os.region 
    FROM "DDS".managers as m, "STG".old_sales as os;
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

def load_orders_realizations_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "orders_realizations"

    # load to local to DB (orders_realization)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "DDS".orders_realizations(order_date, order_number, realization_date, realization_number, item_number, count, price, total_sum, comment)
    SELECT to_date(order_date, 'DD-MM-YYYY'), order_number, to_date(realization_date, 'DD-MM-YYYY'), realization_number, item_number, count, price, total_sum, comment  
    FROM "STG".old_sales
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

def load_sales_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "sales"

    # load to local to DB (sales)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    insert into "DDS".sales(id_manager, client_id, order_number, realization_number, item_number, count, total_sum)
    SELECT m.id, os.client_id, os.order_number, os.realization_number, os.item_number, os.count, os.total_sum 
    FROM "DDS".managers as m, "STG".old_sales as os;
        """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2017, 1, 1),
}

with DAG(
        'from_stg_to_dds_dag',                  
        default_args=default_args,         
        schedule_interval=None,  
        start_date=datetime(2017, 1, 1),  
        catchup=False,                     
        tags=['Pet-Project', 'stg', 'dds'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t11 = PythonOperator(task_id="managers", python_callable=load_managers_to_dds, dag=dag)
    t12 = PythonOperator(task_id="clients", python_callable=load_clients_to_dds, dag=dag)
    t13 = PythonOperator(task_id="orders_realizations", python_callable=load_orders_realizations_to_dds, dag=dag)
    t14 = PythonOperator(task_id="sales", python_callable=load_sales_to_dds, dag=dag)
    t2 = DummyOperator(task_id="end")

    t1 >> t11 >> t12 >> t13 >> t14 >> t2

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
def load_managers_to_dds():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "managers"

    # load to local to DB (managers)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    INSERT INTO "DDS".managers(manager)
    SELECT DISTINCT manager  
    FROM "STG".old_sales
    WHERE NOT EXISTS (
		SELECT 1
		FROM "DDS".managers
		WHERE "DDS".managers.manager = "STG".old_sales.manager 
);
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
    INSERT INTO "DDS".clients(client_id, client, manager_id, sales_channel, region)
    SELECT DISTINCT os.client_id,
           os.client,
           m.id AS manager_id,
           os.sales_channel,
           os.region 
    FROM "STG".old_sales AS os 
    LEFT JOIN "DDS".clients AS c using(client_id) 
    LEFT JOIN "DDS".managers AS m using(manager)
    WHERE NOT EXISTS (
		SELECT 1
		FROM "DDS".clients
		WHERE "DDS".clients.client_id = os.client_id 
)
;
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
    TRUNCATE "DDS".orders_realizations;
    INSERT INTO "DDS".orders_realizations(
	client_id, 
	order_date, 
	order_number, 
	realization_date, 
	realization_number, 
	item_number, 
	count, 
	price, 
	total_sum, 
	comment)
    	SELECT 
    		c.id as client_id, 
    		to_date(os.order_date, 'DD-MM-YYYY'), 
    		os.order_number, 
    		to_date(os.realization_date, 'DD-MM-YYYY'), 
    		os.realization_number, 
    		os.item_number, 
    		os.count, 
    		os.price, 
    		os.total_sum, 
    		os.comment  
	    FROM "STG".old_sales as os
	    left join "DDS".clients as c using(client_id)	    
;
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
    t2 = DummyOperator(task_id="end")

    t1 >> t11 >> t12 >> t13 >> t2

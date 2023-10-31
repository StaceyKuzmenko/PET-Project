import pendulum
import time
import psycopg2
from datetime import datetime, timedelta
from wsgiref import headers
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

### POSTGRESQL settings ###
# init connection
# Connect to your local postgres DB (Docker)

conn = BaseHook.get_connection('postgres_db_conn')

DB_NAME = conn.schema
DB_USER = conn.login
DB_PASS = conn.password
DB_HOST = conn.host
DB_PORT = conn.port


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
    --создаем словарь менеджеров, в который инкрементально добавляются новые менеджеры, которые до этого отсутствовали в базе
    --сначала добавляем менеджеров из старой базы данных (2017-2022 гг.)
    --уникальные (суррогатные) id менеджеров генерируются автоматически
    INSERT INTO "DDS".managers(manager) 
    SELECT 
      DISTINCT manager 
    FROM 
      "STG".old_sales 
    WHERE 
      NOT EXISTS (
        SELECT 
          1 
        FROM 
          "DDS".managers 
        WHERE 
          "DDS".managers.manager = "STG".old_sales.manager
      );
    --добавляем менеджеров из новой базы данных (2023 г.)  
    INSERT INTO "DDS".managers(manager) 
    SELECT 
      DISTINCT manager 
    FROM 
      "STG".sales 
    WHERE 
      NOT EXISTS (
        SELECT 
          1 
        FROM 
          "DDS".managers 
        WHERE 
          "DDS".managers.manager = "STG".sales.manager
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
    --создаем словарь клиентов, в который инкрементально добавляются новые клиенты, которые до этого отсутствовали в базе
    --сначала добавляем клиентов из старой базы данных (2017-2022 гг.)
    --уникальные (суррогатные) id клиентов генерируются автоматически
    --в словаре помимо id и наименования клиентов присутствуют: уникальные id менеджеров (подтягиваются из словаря managers), канал продаж и регион
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
    --добавляем клиентов из новой базы данных (2023 г.)
    INSERT INTO "DDS".clients(client_id, client, manager_id, sales_channel, region)
    SELECT DISTINCT s.client_id,
        s.client,
        m.id AS manager_id,
        s.sales_channel,
        s.region 
    FROM "STG".sales AS s 
    LEFT JOIN "DDS".clients AS c using(client_id) 
    LEFT JOIN "DDS".managers AS m using(manager)
    WHERE NOT EXISTS (
	SELECT 1
	FROM "DDS".clients
        WHERE "DDS".clients.client_id = s.client_id 
    );
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
        --создаем таблицу показателей продаж;
	--для начала в исходных данных на STG слое в таблицах old_sales, sales заменяем разделители целой и дробной части в числовых данных с запятой на точку
        UPDATE "STG".sales
    	SET 
    	    price = REPLACE(price, ',', '.'),
    	    total_sum = REPLACE(total_sum, ',', '.');
    	UPDATE "STG".old_sales
    	SET 
    	    price = REPLACE(price, ',', '.'),
    	    total_sum = REPLACE(total_sum, ',', '.');
    	TRUNCATE "DDS".orders_realizations;
     	--добавляем в таблицу данные из "STG".old_sales
        INSERT INTO "DDS".orders_realizations(
	    client_id, --суррогатный id
	    order_date, --дата заказа
	    order_number, --номер заказа
	    realization_date, --дата отгрузки
	    realization_number, --номер отгрузки
	    item_number, --артикул товара
	    count, --количество
	    price, --цена в заказе
	    total_sum, -сумма заказа
	    comment) --комментарий к заказу
    	    SELECT  
    	        c.id as client_id, 
    		to_date(os.order_date, 'DD-MM-YYYY'), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - date
    		os.order_number, 
    		to_date(os.realization_date, 'DD-MM-YYYY'), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - date
    		os.realization_number, 
    		os.item_number, 
    		os.count, 
    		cast(os.price as double precision), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - numeric
            	cast(os.total_sum as double precision), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - numeric
    		os.comment  
	    FROM "STG".old_sales as os
	    left join "DDS".clients as c using(client_id)	    
        ;
	--добавляем в таблицу актуальные данные из "STG".sales
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
    		to_date(s.order_date, 'DD-MM-YYYY'), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - date
    		s.order_number, 
    		to_date(s.realization_date, 'DD-MM-YYYY'), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - date
    		s.realization_number, 
    		s.item_number, 
    		s.count, 
    		cast(s.price as double precision), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - numeric
            	cast(s.total_sum as double precision), --в исходных данных слоя STG все данные типа varchar; на данном этапе мы переводим эти данные в корректные - numeric
    		s.comment  
	    FROM "STG".sales as s
	    left join "DDS".clients as c using(client_id)	    
        ;
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 16),
}

execution_times = ["15 4", "15 9", "15 13"] # <<< this is in UTC (in UTC +3 07:15; 12:15; 16:15)

with DAG(
        'from_stg_to_dds_dag',                  
        default_args=default_args,         
        schedule_interval='15 4,9,13 * * *',  
        start_date=datetime(2023, 10, 16),  
        catchup=False,                     
        tags=['Pet-Project', 'stg', 'dds'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t11 = PythonOperator(task_id="managers", python_callable=load_managers_to_dds, dag=dag)
    t12 = PythonOperator(task_id="clients", python_callable=load_clients_to_dds, dag=dag)
    t13 = PythonOperator(task_id="orders_realizations", python_callable=load_orders_realizations_to_dds, dag=dag)
    t2 = DummyOperator(task_id="end")

    '''for time in execution_times:
    dag.schedule_interval = f'{time} * * *'
    '''
	
    t1 >> t11 >> t12 >> t13 >> t2


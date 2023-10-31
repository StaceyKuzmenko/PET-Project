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

conn = BaseHook.get_connection('postgres_local')

DB_NAME = conn.schema
DB_USER = conn.login
DB_PASS = conn.password
DB_HOST = conn.host
DB_PORT = conn.port

conn_1 = psycopg2.connect(
    database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)

# load data from DDS
# paste data to CDM local connection
def load_all_months_aggregated_sales():
    try:
        # fetching time UTC and table
        fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_table = "all_months_aggregated_sales"

        # load to local to DB (all_months_aggregated_sales)
        cur_1 = conn_1.cursor()
        postgres_insert_query = """ 
	--заливаем данные в итоговую витрину данных "Агрегированные продажи за весь период (2017-2023 гг.) по месяцам"
 	--сначала заливаем данные по отгрузкам (это товары, у которых есть номер заказа и реализации (отгрузки))
        TRUNCATE "CDM".all_months_aggregated_sales;
        INSERT INTO "CDM".all_months_aggregated_sales (client_id, sales_channel, realization_date, item_number, subbrand, brand, count, total_sum)
        SELECT 
	    or2.client_id,
	    c.sales_channel,
	    or2.realization_date,
	    or2.item_number,
	    stgc.subbrand,
	    stgc.brand,
	    or2.count,
	    or2.total_sum
        FROM "DDS".orders_realizations or2 
        LEFT JOIN "DDS".clients c ON or2.client_id=c.id
        LEFT JOIN "STG".category AS stgc using(item_number)
        WHERE or2.realization_date IS NOT NULL
        ;
	--в таблице "DDS".orders_realizations есть как отгруженные заказы, так и те заказы, которые были созданы, но по каким-то причинам не были отгружены.
 	--неотгруженные заказы (без номер и даты реализации) нам не нужны. Но помимо них, в таблице "DDS".orders_realizations есть еще отчеты комиссионеров. Они также не имеют номера и даты реализации, но они являются отгруженными заказами. Поэтому в таблице они должны присутствовать.
	--сейчас в витрину добавим отчеты комиссионеров (это 2 клиента)
        INSERT INTO "CDM".all_months_aggregated_sales (client_id, sales_channel, realization_date, item_number, subbrand, brand, count, total_sum)
        SELECT 
	    or2.client_id,
	    c.sales_channel,
	    or2.order_date,
	    or2.item_number,
	    stgc.subbrand,
	    stgc.brand,
	    or2.count,
	    or2.total_sum
        FROM "DDS".orders_realizations or2 
        LEFT JOIN "DDS".clients c ON or2.client_id=c.id
        LEFT JOIN "STG".category AS stgc using(item_number)
        WHERE or2.realization_date IS NULL AND (c.client_id = '00-00001081' OR c.client_id = '00-00000883')
        ;
        """
        cur_1.execute(postgres_insert_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error with insert at all_months_aggregated_sales:", error)
    finally:
        # Close the connection
        conn_1.commit()
        conn_1.close()

def load_monthly_sales_by_brands():
    try:
        # fetching time UTC and table
        fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_table = "monthly_sales_by_brands"

        # load to local to DB (monthly_sales_by_brands)
        cur_1 = conn_1.cursor()
        postgres_insert_query = """ 
	--из предыдущей витрины мы создадим дополнительную витрину: "Продажи по брендам, по месяцам"
        TRUNCATE "CDM".monthly_sales_by_brands;
        INSERT INTO "CDM".monthly_sales_by_brands (realization_month, brand, total_sum)
        SELECT
   	    distinct to_char(amas.realization_date, 'YYYY-MM') AS realization_month,	
    	    amas.brand,
            ROUND(CAST(SUM(amas.total_sum) AS numeric), 2) AS total_sum
        FROM "CDM".all_months_aggregated_sales amas 
        GROUP BY realization_month, amas.brand
        ORDER BY to_char(amas.realization_date, 'YYYY-MM');
        """
        cur_1.execute(postgres_insert_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error with insert at monthly_sales_by_brands:", error)
    finally:
        # Close the connection    
        conn_1.commit()
        conn_1.close()

def load_monthly_sales_by_sales_channels():
    try:
        # fetching time UTC and table
        fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_table = "monthly_sales_by_sales_channels"

        # load to local to DB (monthly_sales_by_sales_channels)
        cur_1 = conn_1.cursor()
        postgres_insert_query = """ 
	--из предыдущей витрины мы создадим дополнительную витрину: "Продажи по каналам (продаж), по месяцам"
        TRUNCATE "CDM".monthly_sales_by_sales_channels;
        INSERT INTO "CDM".monthly_sales_by_sales_channels (realization_month, client, sales_channel, total_sum)
        SELECT
  	    distinct to_char(amas.realization_date, 'YYYY-MM') AS realization_month,	
   	    c.client,
            amas.sales_channel,
            ROUND(CAST(SUM(amas.total_sum) AS numeric), 2) AS total_sum
        FROM "CDM".all_months_aggregated_sales amas  
        LEFT JOIN "DDS".clients c ON amas.client_id=c.id
        GROUP BY realization_month, c.client, amas.sales_channel
        ORDER BY to_char(amas.realization_date, 'YYYY-MM');

 	--в данной витрине должны присутствовать отчеты комиссионеров, нам нужно их в нее добавить.
  	--продажи комиссионеров в 1С у нас появляются не сразу после продажи, а с лагом в месяц - когда комиссионер присылает нам отчет.
   	--но руководству нужно знать текущие продажи в данном канале, поэтому мы их запрашиваем у менеджеров; те в файле csv нам предоставляют эти данные
    	--после чего мы загружаем этот файл на STG слой и оттуда забираем в витрину.
     	--при этом, важно сказать, что частично в 1С по одному комиссионеру мы можем в текущем месяце видеть продажи, но они не полные
      	--поэтому нам нужно сравнить продажи в monthly_sales_by_sales_channels и в "STG".marketplaces, и оставить в итоговой витрине бОльшую сумму (она актуальнее)
	with a as(
	select  
		distinct m.client,
		sum(m.total_realizations) as total_realizations
	FROM "STG".marketplaces as m
	group by m.client
	)
	UPDATE "CDM".monthly_sales_by_sales_channels
	SET total_sum = GREATEST("CDM".monthly_sales_by_sales_channels.total_sum, a.total_realizations)
	FROM a
	WHERE "CDM".monthly_sales_by_sales_channels.client = a.client and "CDM".monthly_sales_by_sales_channels.realization_month = '2023-09';

	--при этом, продажи остальных комиссионеров мы просто добавляем в итоговую витрину (по условию, что он остсутствует в итоговой витрине)
	with a as(
	select  
		distinct m.client,
		sum(m.total_realizations) as total_realizations
	FROM "STG".marketplaces as m
	group by m.client
	)
	INSERT INTO "CDM".monthly_sales_by_sales_channels (realization_month, client, sales_channel, total_sum)
	SELECT '2023-09', a.client, 'Шармбатон', a.total_realizations
	FROM a
	WHERE NOT EXISTS (SELECT 1 FROM "CDM".monthly_sales_by_sales_channels AS t1 WHERE t1.client = a.client and t1.realization_month = '2023-09');
        """
        cur_1.execute(postgres_insert_query) 
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error with insert at monthly_sales_by_sales_channels:", error)
    finally:
        # Close the connection    
        conn_1.commit()
        conn_1.close()

def load_to_current_month_aggregated_sales():
    try:
        # fetching time UTC and table
        fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_table = "current_month_aggregated_sales"

        # load to local to DB (current_month_aggregated_sales)
        cur_1 = conn_1.cursor()
        postgres_insert_query = """ 
	--создаем витрину продаж текущего месяца. Эта витрина агрегированна по менеджеру, клиенту и бренду. Она нам необходима для создания итогового отчета по продажам, в котором мы будем сравнивать текущие продажи с прогнозом, который выставило руководство и менеджеры
        truncate "CDM".current_month_aggregated_sales ;
        insert into "CDM".current_month_aggregated_sales(manager, client, subbrand, orders_sum, realizations_sum)
        with a as (
	    select
		distinct or2.client_id,
  		or2.realization_number,
		c.subbrand,
		case
		    when or2.realization_number is null or or2.realization_number = '' then sum(or2.total_sum) --в итоговой витрине мы хотим видеть отдельный столбец по заказам (не отгруженным) и отдельный столбец - по реализациям (отгруженным). Но сумма заказа у нас находится в единственном столбце total_sum, поэтому с помощью конструкции case, мы распределим данные total_sum по отдельным столбцам.
		    else null
		end as orders_sum,
		case
		    when or2.realization_number is not null and or2.realization_number != '' then sum(or2.total_sum)
		    else null
		end as realizations_sum
	    from "DDS".orders_realizations or2
	    left join "STG".category as c using(item_number)
	    where to_char(or2.order_date, 'YYYY-MM') = '2023-09' or to_char(or2.realization_date, 'YYYY-MM') = '2023-09'
	    group by or2.client_id, c.subbrand, or2.realization_number
	    ),
        b as (
	    select
		c2.manager_id,
		a.client_id,
		c2.client,
		a.subbrand,
		sum(a.orders_sum) as orders_sum,
		sum(a.realizations_sum) as realizations_sum
	    from a
	    left join "DDS".clients c2 on a.client_id=c2.id
	    group by a.client_id, c2.client, c2.manager_id, a.subbrand
        )
	    select
		m.manager,
		b.client,
		b.subbrand,
		b.orders_sum,
		b.realizations_sum
	    from b
	    left join "DDS".managers m on b.manager_id=m.id
	    ;
     	--добавляем в витрину данные комиссионеров (сначала сравниваем продажи в "CDM".current_month_aggregated_sales и "STG".marketplaces; выбираем бОльшую сумму продаж комиссионеров, которые присутствуют в "CDM".current_month_aggregated_sales)
        UPDATE "CDM".current_month_aggregated_sales
        SET realizations_sum = GREATEST("CDM".current_month_aggregated_sales.realizations_sum, "STG".marketplaces.total_realizations)
        FROM "STG".marketplaces
        WHERE "CDM".current_month_aggregated_sales.client = "STG".marketplaces.client and "CDM".current_month_aggregated_sales.subbrand = "STG".marketplaces.brand;
        --добавляем в витрину данные продаж комиссионеров из "STG".marketplaces, которых нет в итоговой витрине
	INSERT INTO "CDM".current_month_aggregated_sales (manager, client, subbrand, realizations_sum)
        SELECT t2.manager, t2.client, t2.brand, t2.total_realizations
        FROM "STG".marketplaces AS t2
        WHERE NOT EXISTS (SELECT 1 FROM "CDM".current_month_aggregated_sales AS t1 WHERE t1.manager = t2.manager and t1.client = t2.client and t1.subbrand = t2.brand);
        """
        cur_1.execute(postgres_insert_query)  
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error with insert at current_month_aggregated_sales:", error)
    finally:
        # Close the connection    
        conn_1.commit()
        conn_1.close()

def load_to_monthly_sales_report():
    try:
        # fetching time UTC and table
        fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_table = "monthly_sales_report"

        # load to local to DB (monthly_sales_report)
        cur_1 = conn_1.cursor()
        postgres_insert_query = """ 
	--создаем итоговую витрину "Отчета по продажам"
        truncate "CDM".monthly_sales_report;
        with e as (
	    select
		f.manager,
		f.client,
		f.brand,
		f.general_plan,
		f.week_1,
		f.week_2,
		f.week_3,
		f.week_4,
		f.week_5
	    from "STG".forecast f
        )
        insert into "CDM".monthly_sales_report(manager, client, brand, general_plan, plan_comletion_percent, orders_sum, realizations_sum, total_sum, week_1, week_2, week_3, week_4, week_5, general_forecast, forecast_completion_percent)
        select
	    e.manager, --менеджер
	    e.client, --клиент
	    e.brand, --бренд
	    e.general_plan, --план, выставленный руководством
	    CASE
        WHEN e.general_plan <= 0 THEN 0 -- Handle division by zero or negative revenue
        ELSE (cmas.orders_sum + cmas.realizations_sum) / e.general_plan
        END AS plan_completion_percent, --выполнение плана, выставленного руководством (относительно текущих продаж)
	    cmas.orders_sum, --сумма заказов
	    cmas.realizations_sum, --сумма отгрузок
	    cmas.orders_sum + cmas.realizations_sum as total_sum, --общая сумма заказов и отгрузок
	    e.week_1, --план продаж менеджеров на 1 неделю
	    e.week_2, --план продаж менеджеров на 2 неделю
	    e.week_3, --план продаж менеджеров на 3 неделю
	    e.week_4, --план продаж менеджеров на 4 неделю
	    e.week_5, --план продаж менеджеров на 5 неделю
	    (e.week_1 +	e.week_2 + e.week_3 + e.week_4 + e.week_5) as general_forecast, - общий прогноз продаж менеджеров на месяц
	    CASE
        WHEN e.general_plan <= 0 THEN 0 -- Handle division by zero or negative revenue
        ELSE (e.week_1 + e.week_2 + e.week_3 + e.week_4 + e.week_5) / e.general_plan
        END AS forecast_completion_percent --выполнение плана продаж, выставленного руководством (в сравнении с прогнозом продаж, выставленного менеджерами)
        from e
        left join "CDM".current_month_aggregated_sales cmas on e.manager=cmas.manager and e.client=cmas.client and e.brand=cmas.subbrand
        group by e.manager,
            e.client,
	    e.brand,
	    e.general_plan,
	    cmas.orders_sum,
	    cmas.realizations_sum,
	    e.week_1,
	    e.week_2,
	    e.week_3,
	    e.week_4,
	    e.week_5
        ;
	--добавляем в витрину "Отчет по продажам" новых менеджеров, клиентов, бренды, которых изначально не было в витрине
        INSERT INTO "CDM".monthly_sales_report (manager, client, brand, orders_sum, realizations_sum)
        SELECT 
	    t2.manager,
	    t2.client, 
	    t2.subbrand,
	    t2.orders_sum,
	    t2.realizations_sum
        FROM "CDM".current_month_aggregated_sales AS t2
        WHERE NOT EXISTS (SELECT 1 FROM "CDM".monthly_sales_report AS t1 WHERE t1.manager = t2.manager and t1.client = t2.client and t1.brand = t2.subbrand);
        """
        cur_1.execute(postgres_insert_query) 
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error with insert at monthly_sales_report:", error)
    finally:
        # Close the connection    
        conn_1.commit()
        conn_1.close()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 16),
}

execution_times = ["20 4", "20 9", "20 13"] # <<< this is in UTC (in UTC +3 07:20; 12:20; 16:20)

with DAG(
        'from_dds_to_cdm_dag',                  
        default_args=default_args,         
        schedule_interval='20 4,9,13 * * *',  
        start_date=datetime(2023, 10, 16),  
        catchup=False,                     
        tags=['Pet-Project', 'dds', 'cdm'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t10 = PythonOperator(task_id="all_months_to", python_callable=load_all_months_aggregated_sales, dag=dag)
    t11 = PythonOperator(task_id="brands_to", python_callable=load_monthly_sales_by_brands, dag=dag)
    t12 = PythonOperator(task_id="channels_to", python_callable=load_monthly_sales_by_sales_channels, dag=dag)
    t13 = PythonOperator(task_id="aggregates_sales_to", python_callable=load_to_current_month_aggregated_sales, dag=dag)
    t14 = PythonOperator(task_id="forecast_to", python_callable=load_to_monthly_sales_report, dag=dag)
    t2 = DummyOperator(task_id="end")

    '''for time in execution_times:
    dag.schedule_interval = f'{time} * * *'
    '''
    
    t1 >> t10 >> t11 >> t12 >> t13 >> t14 >> t2


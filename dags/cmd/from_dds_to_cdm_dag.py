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

#DB_NAME = "project_db"
#DB_USER = "project_user"
#DB_PASS = "project_password"
#DB_HOST = "91.107.126.62"
#DB_PORT = "5433"

#conn_1 = psycopg2.connect(
#    database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
#)

conn_1 = psycopg2.getconn('postgres_db_conn')

# load data from DDS
# paste data to CDM local connection
def load_monthly_sales_by_brands():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_by_brands"

    # load to local to DB (monthly_sales_by_brands)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    TRUNCATE "CDM".monthly_sales_by_brands;
    INSERT INTO "CDM".monthly_sales_by_brands (realization_month, brand, total_sum)
    SELECT
    	distinct to_char(or2.realization_date, 'YYYY-MM') AS realization_month,	
	    c.brand,
	    ROUND(CAST(SUM(or2.total_sum) AS numeric), 2) AS total_sum
    FROM "DDS".orders_realizations or2 
    LEFT JOIN "STG".category c USING(item_number)
    GROUP BY realization_month, brand
    ORDER BY to_char(or2.realization_date, 'YYYY-MM');
    """
    cur_1.execute(postgres_insert_query)
    conn_1.commit()
    conn_1.close()

def load_monthly_sales_by_sales_channels():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_by_sales_channels"

    # load to local to DB (monthly_sales_by_sales_channels)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    TRUNCATE "CDM".monthly_sales_by_sales_channels;
    INSERT INTO "CDM".monthly_sales_by_sales_channels (realization_month, client, sales_channel, total_sum)
    SELECT
    	distinct to_char(or2.realization_date, 'YYYY-MM') AS realization_month,	
    	c.client,
	    c.sales_channel,
	    ROUND(CAST(SUM(or2.total_sum) AS numeric), 2) AS total_sum
    FROM "DDS".orders_realizations or2 
    LEFT JOIN "DDS".clients c ON or2.client_id=c.id
    GROUP BY realization_month, c.client, sales_channel
    ORDER BY to_char(or2.realization_date, 'YYYY-MM');
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

def load_to_current_month_aggregated_sales():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "current_month_aggregated_sales"

    # load to local to DB (current_month_aggregated_sales)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    truncate "CDM".current_month_aggregated_sales ;
    insert into "CDM".current_month_aggregated_sales(manager, client, subbrand, orders_sum, realizations_sum)
    with a as (
	select
		distinct or2.client_id,
		c.subbrand,
		case
			when or2.realization_number is null or or2.realization_number = '' then sum(or2.total_sum)
			else null
		end as orders_sum,
		case
			when or2.realization_number is not null and or2.realization_number != '' then sum(or2.total_sum)
			else null
		end as realizations_sum
	from "DDS".orders_realizations or2
	left join "STG".category as c using(item_number)
	where to_char(or2.order_date, 'YYYY-MM') = '2023-09'
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
		case when b.orders_sum is null then 0 else b.orders_sum end,
		case when b.realizations_sum is null then 0 else b.realizations_sum end
	from b
	left join "DDS".managers m on b.manager_id=m.id
	;
    UPDATE "CDM".current_month_aggregated_sales
    SET realizations_sum = GREATEST("CDM".current_month_aggregated_sales.realizations_sum, "STG".marketplaces.total_realizations)
    FROM "STG".marketplaces
    WHERE "CDM".current_month_aggregated_sales.client = "STG".marketplaces.client and "CDM".current_month_aggregated_sales.subbrand = "STG".marketplaces.brand;
    INSERT INTO "CDM".current_month_aggregated_sales (manager, client, subbrand, realizations_sum)
    SELECT t2.manager, t2.client, t2.brand, t2.total_realizations
    FROM "STG".marketplaces AS t2
    WHERE NOT EXISTS (SELECT 1 FROM "CDM".current_month_aggregated_sales AS t1 WHERE t1.manager = t2.manager and t1.client = t2.client and t1.subbrand = t2.brand);
    """
    cur_1.execute(postgres_insert_query)    
    conn_1.commit()
    conn_1.close()

def load_to_monthly_sales_report():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_table = "monthly_sales_report"

    # load to local to DB (monthly_sales_report)
    cur_1 = conn_1.cursor()
    postgres_insert_query = """ 
    truncate "CDM".monthly_sales_report;
    with e as (
	    select
		    f.manager,
		    f.client,
		    f.brand,
		    case when f.general_plan is null then 0 else f.general_plan end,
		    case when f.week_1 is null then 0 else f.week_1 end,
		    case when f.week_2 is null then 0 else f.week_2 end,
		    case when f.week_3 is null then 0 else f.week_3 end,
		    case when f.week_4 is null then 0 else f.week_4 end,
		    case when f.week_5 is null then 0 else f.week_5 end
	    from "STG".forecast f
    )
    insert into "CDM".monthly_sales_report(manager, client, brand, general_plan, plan_comletion_percent, orders_sum, realizations_sum, total_sum, week_1, week_2, week_3, week_4, week_5, general_forecast, forecast_completion_percent)
    select
	    e.manager,
	    e.client,
	    e.brand,
	    e.general_plan,
	    CASE
    WHEN e.general_plan <= 0 THEN 0 -- Handle division by zero or negative revenue
    ELSE (cmas.orders_sum + cmas.realizations_sum) / e.general_plan
    END AS plan_completion_percent,
	    case when cmas.orders_sum is null then 0 else cmas.orders_sum end, --done
	    case when cmas.realizations_sum is null then 0 else cmas.realizations_sum end,	--done
	    cmas.orders_sum + cmas.realizations_sum as total_sum, --done
	    e.week_1,
	    e.week_2,
	    e.week_3,
	    e.week_4,
	    e.week_5,
	    (e.week_1 +	e.week_2 + e.week_3 + e.week_4 + e.week_5) as general_forecast,
	    CASE
    WHEN e.general_plan <= 0 THEN 0 -- Handle division by zero or negative revenue
    ELSE (e.week_1 + e.week_2 + e.week_3 + e.week_4 + e.week_5) / e.general_plan
    END AS forecast_completion_percent
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
    INSERT INTO "CDM".monthly_sales_report (manager, client, brand, orders_sum, realizations_sum)
    SELECT 
	    t2.manager,
	    t2.client, 
	    t2.subbrand, 
	    case when t2.orders_sum is null then 0 else t2.orders_sum end,
	    case when t2.realizations_sum is null then 0 else t2.realizations_sum end
    FROM "CDM".current_month_aggregated_sales AS t2
    WHERE NOT EXISTS (SELECT 1 FROM "CDM".monthly_sales_report AS t1 WHERE t1.manager = t2.manager and t1.client = t2.client and t1.brand = t2.subbrand);
    """
    cur_1.execute(postgres_insert_query)  
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
    t11 = PythonOperator(task_id="brands_to", python_callable=load_monthly_sales_by_brands, dag=dag)
    t12 = PythonOperator(task_id="channels_to", python_callable=load_monthly_sales_by_sales_channels, dag=dag)
    t13 = PythonOperator(task_id="aggregates_sales_to", python_callable=load_to_current_month_aggregated_sales, dag=dag)
    t14 = PythonOperator(task_id="forecast_to", python_callable=load_to_monthly_sales_report, dag=dag)
    t2 = DummyOperator(task_id="end")

    '''for time in execution_times:
    dag.schedule_interval = f'{time} * * *'
    '''
    
    t1 >> t11 >> t12 >> t13 >> t14 >> t2


--DROP TABLE IF EXISTS "STG".test_sales;
CREATE TABLE "STG".sales (
	manager_id int4 NULL,
	manager varchar NULL,
	client_id varchar NOT NULL,
	client varchar NOT NULL,
	sales_channel varchar NULL,
	region varchar NULL,
	order_date varchar NULL,
	order_number varchar NULL,
	realization_date varchar NULL,
	realization_number varchar NULL,
	product_id varchar NOT NULL,
	item_number varchar NOT NULL,
	product_name varchar NOT NULL,
	brand varchar NULL,
	count int4 NULL,
	price numeric(14, 2) NULL,
	total_sum numeric(14, 2) NULL,
	"comment" varchar NULL
);

--DROP TABLE IF EXISTS "STG".test_category;
CREATE TABLE "STG".category (
	item_number varchar NOT NULL,
	product_name varchar NOT NULL,
	brand varchar NOT NULL,
	product_category varchar NULL
);

--DROP TABLE IF EXISTS "STG".test_forecast;
CREATE TABLE "STG".forecast (
	manager varchar NOT NULL,
	client_id varchar NOT NULL,
	client varchar NOT NULL,
	brand varchar NOT NULL,
	general_plan numeric(14, 2) NULL,
	week_1 numeric(14, 2) NULL,
	week_2 numeric(14, 2) NULL,
	week_3 numeric(14, 2) NULL,
	week_4 numeric(14, 2) NULL,
	week_5 numeric(14, 2) NULL
);


CREATE TABLE "STG".old_sales (
	manager_id int4 NULL,
	manager varchar NULL,
	client_id varchar NOT NULL,
	client varchar NOT NULL,
	sales_channel varchar NULL,
	region varchar NULL,
	order_date varchar NULL,
	order_number varchar NULL,
	realization_date varchar NULL,
	realization_number varchar NULL,
	product_id varchar NOT NULL,
	item_number varchar NOT NULL,
	product_name varchar NOT NULL,
	brand varchar NULL,
	count int4 NULL,
	price numeric(14, 2) NULL,
	total_sum numeric(14, 2) NULL,
	"comment" varchar NULL
);
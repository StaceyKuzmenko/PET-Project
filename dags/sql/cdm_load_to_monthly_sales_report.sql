--создаем итоговую витрину "Отчета по продажам"
TRUNCATE "CDM".monthly_sales_report;
WITH e AS (
    SELECT
		f.manager,
		f.client,
		f.brand,
		f.general_plan,
		f.week_1,
		f.week_2,
		f.week_3,
		f.week_4,
		f.week_5
	FROM "STG".forecast f
    )
INSERT INTO "CDM".monthly_sales_report(manager, client, brand, general_plan, plan_comletion_percent, orders_sum, realizations_sum, total_sum, week_1, week_2, week_3, week_4, week_5, general_forecast, forecast_completion_percent)
SELECT
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
    cmas.orders_sum + cmas.realizations_sum AS total_sum, --общая сумма заказов и отгрузок
    e.week_1, --план продаж менеджеров на 1 неделю
    e.week_2, --план продаж менеджеров на 2 неделю
    e.week_3, --план продаж менеджеров на 3 неделю
    e.week_4, --план продаж менеджеров на 4 неделю
    e.week_5, --план продаж менеджеров на 5 неделю
    (e.week_1 +	e.week_2 + e.week_3 + e.week_4 + e.week_5) AS general_forecast, --общий прогноз продаж менеджеров на месяц
    CASE
        WHEN e.general_plan <= 0 THEN 0 -- Handle division by zero or negative revenue
        ELSE (e.week_1 + e.week_2 + e.week_3 + e.week_4 + e.week_5) / e.general_plan
    END AS forecast_completion_percent --выполнение плана продаж, выставленного руководством (в сравнении с прогнозом продаж, выставленного менеджерами)
FROM e
LEFT JOIN "CDM".current_month_aggregated_sales cmas ON e.manager=cmas.manager AND e.client=cmas.client AND e.brand=cmas.subbrand
GROUP BY 
    e.manager,
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
WHERE NOT EXISTS (SELECT 1 FROM "CDM".monthly_sales_report AS t1 WHERE t1.manager = t2.manager AND t1.client = t2.client AND t1.brand = t2.subbrand);
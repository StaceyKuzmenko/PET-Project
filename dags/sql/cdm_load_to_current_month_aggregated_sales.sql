--создаем витрину продаж текущего месяца. Эта витрина агрегированна по менеджеру, клиенту и бренду. Она нам необходима для создания итогового отчета по продажам, в котором мы будем сравнивать текущие продажи с прогнозом, который выставило руководство и менеджеры
TRUNCATE "CDM".current_month_aggregated_sales ;
INSERT INTO "CDM".current_month_aggregated_sales(manager, client, subbrand, orders_sum, realizations_sum)
WITH a AS (
    SELECT
        DISTINCT or2.client_id,
  	    or2.realization_number,
	    c.subbrand,
	    CASE
	        WHEN or2.realization_number IS NULL OR or2.realization_number = '' THEN sum(or2.total_sum) --в итоговой витрине мы хотим видеть отдельный столбец по заказам (не отгруженным) и отдельный столбец - по реализациям (отгруженным). Но сумма заказа у нас находится в единственном столбце total_sum, поэтому с помощью конструкции case, мы распределим данные total_sum по отдельным столбцам.
	        ELSE NULL
	    END AS orders_sum,
	    CASE
	        WHEN or2.realization_number IS NOT NULL AND or2.realization_number != '' THEN sum(or2.total_sum)
	        ELSE NULL
	    END AS realizations_sum
	FROM "DDS".orders_realizations or2
	LEFT JOIN "STG".category AS c USING(item_number)
	WHERE to_char(or2.order_date, 'YYYY-MM') = '2023-09' OR to_char(or2.realization_date, 'YYYY-MM') = '2023-09'
	GROUP BY or2.client_id, c.subbrand, or2.realization_number
	),
b AS (
    SELECT
    	c2.manager_id,
	    a.client_id,
		c2.client,
		a.subbrand,
		sum(a.orders_sum) AS orders_sum,
		sum(a.realizations_sum) AS realizations_sum
    FROM a
    LEFT JOIN "DDS".clients c2 ON a.client_id=c2.id
    GROUP BY a.client_id, c2.client, c2.manager_id, a.subbrand
    )
SELECT
	m.manager,
	b.client,
	b.subbrand,
	b.orders_sum,
	b.realizations_sum
FROM b
LEFT JOIN "DDS".managers m ON b.manager_id=m.id
;
--добавляем в витрину данные комиссионеров (сначала сравниваем продажи в "CDM".current_month_aggregated_sales и "STG".marketplaces; выбираем бОльшую сумму продаж комиссионеров, которые присутствуют в "CDM".current_month_aggregated_sales)
UPDATE "CDM".current_month_aggregated_sales
SET realizations_sum = GREATEST("CDM".current_month_aggregated_sales.realizations_sum, "STG".marketplaces.total_realizations)
FROM "STG".marketplaces
WHERE "CDM".current_month_aggregated_sales.client = "STG".marketplaces.client AND "CDM".current_month_aggregated_sales.subbrand = "STG".marketplaces.brand;
--добавляем в витрину данные продаж комиссионеров из "STG".marketplaces, которых нет в итоговой витрине
INSERT INTO "CDM".current_month_aggregated_sales (manager, client, subbrand, realizations_sum)
SELECT t2.manager, t2.client, t2.brand, t2.total_realizations
FROM "STG".marketplaces AS t2
WHERE NOT EXISTS (SELECT 1 FROM "CDM".current_month_aggregated_sales AS t1 WHERE t1.manager = t2.manager AND t1.client = t2.client AND t1.subbrand = t2.brand);
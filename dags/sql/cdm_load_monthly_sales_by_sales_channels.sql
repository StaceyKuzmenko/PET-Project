--из предыдущей витрины мы создадим дополнительную витрину: "Продажи по каналам (продаж), по месяцам"
TRUNCATE "CDM".monthly_sales_by_sales_channels;
INSERT INTO "CDM".monthly_sales_by_sales_channels (realization_month, client, sales_channel, total_sum)
SELECT
    DISTINCT to_char(amas.realization_date, 'YYYY-MM') AS realization_month,	
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
WITH a AS(
    SELECT  
	    DISTINCT m.client,
	    sum(m.total_realizations) as total_realizations
    FROM "STG".marketplaces as m
    GROUP BY m.client
    )
UPDATE "CDM".monthly_sales_by_sales_channels
SET total_sum = GREATEST("CDM".monthly_sales_by_sales_channels.total_sum, a.total_realizations)
FROM a
WHERE "CDM".monthly_sales_by_sales_channels.client = a.client and "CDM".monthly_sales_by_sales_channels.realization_month = '2023-09';

--при этом, продажи остальных комиссионеров мы просто добавляем в итоговую витрину (по условию, что он остсутствует в итоговой витрине)
WITH a AS(
    SELECT  
	    DISTINCT m.client,
	    sum(m.total_realizations) as total_realizations
    FROM "STG".marketplaces as m
    GROUP BY m.client
    )
INSERT INTO "CDM".monthly_sales_by_sales_channels (realization_month, client, sales_channel, total_sum)
SELECT '2023-09', a.client, 'Шармбатон', a.total_realizations
FROM a
WHERE NOT EXISTS (SELECT 1 FROM "CDM".monthly_sales_by_sales_channels AS t1 WHERE t1.client = a.client and t1.realization_month = '2023-09');
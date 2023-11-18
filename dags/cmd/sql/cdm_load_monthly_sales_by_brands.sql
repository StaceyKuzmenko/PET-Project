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
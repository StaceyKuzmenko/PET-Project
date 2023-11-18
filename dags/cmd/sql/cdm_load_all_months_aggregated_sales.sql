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
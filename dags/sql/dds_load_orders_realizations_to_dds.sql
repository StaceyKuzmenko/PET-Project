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
    total_sum, --сумма заказа
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
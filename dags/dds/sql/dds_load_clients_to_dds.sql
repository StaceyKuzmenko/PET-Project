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
    );
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
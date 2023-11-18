--создаем словарь менеджеров, в который инкрементально добавляются новые менеджеры, которые до этого отсутствовали в базе
--сначала добавляем менеджеров из старой базы данных (2017-2022 гг.)
--уникальные (суррогатные) id менеджеров генерируются автоматически
INSERT INTO "DDS".managers(manager) 
SELECT 
  DISTINCT manager 
FROM 
  "STG".old_sales 
WHERE 
  NOT EXISTS (
    SELECT 
      1 
    FROM 
      "DDS".managers 
    WHERE 
      "DDS".managers.manager = "STG".old_sales.manager
  );
--добавляем менеджеров из новой базы данных (2023 г.)  
INSERT INTO "DDS".managers(manager) 
SELECT 
  DISTINCT manager 
FROM 
  "STG".sales 
WHERE 
  NOT EXISTS (
    SELECT 
      1 
    FROM 
      "DDS".managers 
    WHERE 
      "DDS".managers.manager = "STG".sales.manager
  );
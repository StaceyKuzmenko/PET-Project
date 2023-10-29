--чистим таблицы слоя STG перед тем, как залить в них актуальные данные из локальных папок, которые уже находятся на сервере
TRUNCATE TABLE "STG".sales;
TRUNCATE TABLE "STG".category;
TRUNCATE TABLE "STG".forecast;
TRUNCATE TABLE "STG".marketplaces;

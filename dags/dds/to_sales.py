import logging
from logging import Logger
from typing import List, Optional
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


log = logging.getLogger(__name__)


class SaleRawObj(BaseModel):
    manager_id: int
    manager: varchar
    client_id: varchar
    client: varchar
    sales_channel: varchar
    region: varchar
    order_date: varchar
    order_number: varchar
    realization_date: varchar
    realization_number: varchar
    product_id: varchar
    item_number: varchar
    product_name: varchar
    brand: varchar
    count: int
    price: numeric(14, 2)
    total_sum: numeric(14, 2)
    comment: varchar


class SaleDdsObj(BaseModel):
    client_id: varchar
    order_number: varchar
    realization_number: varchar
    item_number: varchar
    count: int
    total_sum: numeric(14, 2)


class ManagerRawObj(BaseModel):
    manager_id: int
    manager: varchar


class ManagerDdsObj(BaseModel):
    manager: varchar


class ManagerRawRepository:
    def load_raw_manager(self, conn: Connection) -> List[ManagerRawObj]:
        with conn.cursor(row_factory=class_row(ManagerRawObj)) as cur:
            cur.execute(
                """
                    SELECT
                        manager
                    FROM stg.new_sales
                """,
            )
            objs = cur.fetchall()
        return objs


class ManagerDdsRepository:
    def insert_manager(self, conn: Connection, manager: ManagerDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
		    SELECT id
                    FROM dds.managers(id)
                    INSERT INTO dds.sales(id_manager)
                    VALUES (%(id_manager)s)
                    
                """,
                {
                    "id_manager": sales.id_manager,
                },
            )


class SaleRawRepository:
    def load_raw_sale(self, conn: Connection) -> List[SaleRawObj]:
        with conn.cursor(row_factory=class_row(SaleRawObj)) as cur:
            cur.execute(
                """
                    SELECT
                            client_id,
                            order_number, 
                            realization_number, 
                            item_number, 
                            count,
                            total_sum
                    FROM stg.new_sales
                """,
            )
            objs = cur.fetchall()
        return objs


class SaleDdsRepository:
    def insert_client(self, conn: Connection, sale: SaleDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.sales(client_id, order_number, realization_number, item_number, count, total_sum, comment)
                    VALUES (%(order_date)s, %(order_number)s, %(realization_number)s, %(item_number)s, %(count)s, %(total_sum)s)
                    
                """,
                {
                    "client_id": sales.client_id,
                    "order_number": sales.order_number,
                    "realization_number": sales.realization_number,
                    "item_number": sales.item_number,
                    "count": sales.count,
                    "total_sum": sales.total_sum,
                },
            )


class SaleLoader:
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = SaleDdsRepository()
        self.raw = SaleRawRepository()
        self.log = log


class ManagerLoader:
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = ManagerDestRepository()
        self.raw = ManagerRawRepository()
        self.log = log

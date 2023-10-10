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
    order_date: varchar
    order_number: varchar
    realization_date: varchar
    realization_number: varchar
    item_number: varchar
    count: int
    price: numeric(14, 2)
    total_sum: numeric(14, 2)
    comment: varchar


class SaleRawRepository:
    def load_raw_sale(self, conn: Connection) -> List[SaleRawObj]:
        with conn.cursor(row_factory=class_row(SaleRawObj)) as cur:
            cur.execute(
                """
                    SELECT
                            order_date,
                            order_number, 
                            realization_date, 
                            realization_number, 
                            item_number, 
                            count,
                            price,
                            total_sum,
                            comment
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
                    INSERT INTO dds.sales(order_date, order_number, realization_date, realization_number, item_number, count, price, total_sum, comment)
                    VALUES (%(order_date)s, %(order_number)s, %(realization_date)s, %(realization_number)s, %(item_number)s, %(count)s, %(price)s, %(total_sum)s, %(comment)s)
                    
                """,
                {
                    "order_date": sales.order_date,
                    "order_number": sales.order_number,
                    "realization_date": sales.realization_date,
                    "realization_number": sales.realization_number,
                    "item_number": sales.item_number,
                    "count": sales.count,
                    "price": sales.price,
                    "total_sum": sales.total_sum,
                    "comment": sales.comment,
                },
            )


class SaleLoader:
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = SaleDdsRepository()
        self.raw = SaleRawRepository()
        self.log = log


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
    comment: varchar null


class ManagerRawObj(BaseModel):
    manager_id: int
    manager: varchar


class ManagerDdsObj(BaseModel):
    manager_id: int
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
                    INSERT INTO dds.managers(manager)
                    VALUES (%(manager)s)
                    
                """,
                {
                    "manager": manager.manager,
               },
            )


class ManagerLoader:
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = ManagerDestRepository()
        self.raw = ManagerRawRepository()        
        self.log = log




class ClientRawRepository:
    def load_raw_client(self, conn: Connection) -> List[ClientRawObj]:
    with conn.cursor(row_factory=class_row(ClientRawObj)) as cur:
            cur.execute(
                """
                    SELECT
                        client_id,
                        client,
                        sales_channel,
                        region
                    FROM stg.new_sales
                """,
                )
            objs = cur.fetchall()
        return objs


class ClientDdsRepository:
    def insert_client(self, conn: Connection, client: ClientDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.clients(client_id, client, sales_channel, region)
                    VALUES (%(client_id)s, %(client)s, %(sales_channel)s, %(region)s)
                    
                """,
                {
                    "client_id": clients.client_id,
                    "client": clients.client,
                    "sales_channel": clients.sales_channel,
                    "region": clients.region 
                },
            )


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
                    INSERT INTO dds.clients(manager)
                    VALUES (%(manager)s)
                    
                """,
                {
                    "manager": clients.manager,
               },
            )


class ClientLoader:
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = CourierDestRepository()
        self.raw = CourierRawRepository()        
        self.log = log


class ManagerLoader:
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = ManagerDestRepository()
        self.raw = ManagerRawRepository()        
        self.log = log


import logging
from logging import Logger
from typing import List, Optional
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


log = logging.getLogger(__name__)


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

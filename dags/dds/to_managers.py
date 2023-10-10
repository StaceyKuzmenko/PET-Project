import logging
import psycopg2
from logging import Logger
from typing import List, Optional
from library import PgConnect
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

def connect():
    conn = psycopg2.connect(dbname="project_db", host="95.143.191.48", user="project_user", password="project_password", port="5433")
    cur = conn.cursor()

log = logging.getLogger(__name__)


class ManagerRawObj(BaseModel):
    manager_id: int
    manager: varchar


class ManagerDdsObj(BaseModel):
    manager_id: int
    manager: varchar


class ManagerRawRepository:
    def load_raw_manager(self, conn: connect()) -> List[ManagerRawObj]:
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
    def insert_manager(self, conn: connect(), manager: ManagerDdsObj) -> None:
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
    def __init__(self, conn: connect(), log: Logger) -> None:
        self.conn = conn
        self.dds = ManagerDestRepository()
        self.raw = ManagerRawRepository()
        self.log = log

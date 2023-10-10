from logging import Logger
from typing import List
import json
from typing import List, Optional
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
import logging

log = logging.getLogger(__name__)

class ClientRawObj(BaseModel):
    client_id: varchar
    client: varchar
    sales_channel: varchar
    region: varchar

class ClientDdsObj(BaseModel):
    id: int
    id_manager: int # это поле берем из dds.managers
    client_id: varchar
    client: varchar 
	  sales_channel: varchar
	  region: varchar 

class ClientRawRepository:
#    def load_raw_client(self, conn: Connection, last_loaded_record_id: int) -> List[CourierJsonObj]:
#        with conn.cursor(row_factory=class_row(CourierJsonObj)) as cur:
    def load_raw_client(self, conn: Connection, last_loaded_record_id: int) -> List[ClientObj]:
    with conn.cursor(row_factory=class_row(ClientObj)) as cur:
            cur.execute(
                """
                    SELECT
                        client_id,
                        client,
                        sales_channel,
                        region
                    FROM stg.couriers
                """,
                )
            objs = cur.fetchall()
        return objs

class ClientDestRepository:

    def insert_client(self, conn: Connection, client: ClientDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.clients(client_id, client, sales_channel, region)
                    VALUES (%(client_id)s, %(client)s, %(sales_channel)s, %(region)s)
                    
                """,
                {
                    "client_id": clients.client_id,
                    "client": client.client,
                    "sales_channel": client.sales_channel,
                    "region": client.region 
                },
            )



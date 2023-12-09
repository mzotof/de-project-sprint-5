from datetime import datetime, timedelta
from logging import Logger, getLogger

import requests
from airflow.providers.http.hooks.http import HttpHook
from psycopg import Connection
from typing import Any, List, Dict

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.dict_util import str2json, json2str
from lib import ConnectionBuilder, PgConnect


class ApiReader:
    def __init__(self, http_connection_id: str = 'API_COURIER_SYSTEM', nickname: str = 'mzotof', cohort: int = 19) -> None:
        connection = HttpHook.get_connection(http_connection_id)
        self.host = connection.host
        self.headers = {
            'X-Nickname': nickname,
            'X-Cohort': str(cohort),
            'X-API-KEY': connection.extra_dejson.get('api_key'),
        }

    def get_couriers(self, sort_field: str = '_id', sort_direction: str = 'asc', limit: int = 50, offset: int = 0) -> List[Dict]:
        response = requests.get(
            url=f'{self.host}/couriers',
            headers=self.headers,
            params={
                'sort_field': sort_field,
                'sort_direction': sort_direction,
                'limit': str(limit),
                'offset': str(offset),
            }
        )
        return str2json(response.content)

    def get_deliveries(self, from_ts: str, sort_field: str = '_id', sort_direction: str = 'asc', limit: int = 50, offset: int = 0) -> List[Dict]:
        response = requests.get(
            url=f'{self.host}/deliveries',
            headers=self.headers,
            params={
                'sort_field': sort_field,
                'sort_direction': sort_direction,
                'limit': str(limit),
                'offset': str(offset),
                'from': from_ts,
            }
        )
        return str2json(response.content)


class PgSaver:

    def save_courier(self, conn: Connection, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                f'''
                    INSERT INTO stg.couriersystem_couriers (object_id, object_value, processed_ts)
                    VALUES (%(id)s, %(val)s, %(processed_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        processed_ts = case 
                            when EXCLUDED.object_value <> couriersystem_couriers.object_value 
                            then EXCLUDED.processed_ts 
                            else couriersystem_couriers.processed_ts 
                        end;
                ''',
                {
                    'id': val['_id'],
                    'val': str_val,
                    'processed_ts': datetime.now()
                }
            )

    def save_delivery(self, conn: Connection, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                f'''
                    INSERT INTO stg.couriersystem_deliveries (object_id, object_value, delivery_ts)
                    VALUES (%(id)s, %(val)s, %(delivery_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        delivery_ts = EXCLUDED.delivery_ts;
                ''',
                {
                    'id': val['delivery_id'],
                    'val': str_val,
                    'delivery_ts': val['delivery_ts']
                }
            )


class Loader:
    def __init__(self, api_reader: ApiReader = None, pg_dest: PgConnect = None, logger: Logger = None) -> None:
        self.api_reader = api_reader or ApiReader()
        self.pg_saver = PgSaver()
        self.pg_dest = pg_dest or ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger or getLogger(__name__)

    def run_copy_couriers(self, sort_field: str = '_id', sort_direction: str = 'asc', limit: int = 50):
        with self.pg_dest.connection() as conn:
            offset = 0
            while True:
                load_queue = self.api_reader.get_couriers(sort_field, sort_direction, limit, offset)
                self.log.info(f'Found {len(load_queue)} documents to sync from api.')
                if not load_queue:
                    self.log.info('Finishing work. Quitting.')
                    return
                else:
                    offset += limit

                for i, d in enumerate(load_queue):
                    self.pg_saver.save_courier(conn, d)

    def run_copy_deliveries(self, sort_field: str = '_id', sort_direction: str = 'asc', limit: int = 50):
        wf_key = 'courier_system_deliveries'
        last_delivery_ts_key = 'last_delivery_ts'

        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, wf_key)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=wf_key,
                    workflow_settings={last_delivery_ts_key: datetime(2023, 12, 1).strftime('%Y-%m-%d %H:%M:%S')}
                )
            last_delivery_ts_str = wf_setting.workflow_settings[last_delivery_ts_key]
            last_delivery_ts = datetime.strptime(last_delivery_ts_str, '%Y-%m-%d %H:%M:%S')
            self.log.info(f'starting to load from last checkpoint: {last_delivery_ts}')

            next_delivery_ts_str = (last_delivery_ts + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            max_delivery_ts_str = last_delivery_ts_str

            offset = 0
            while True:
                load_queue = self.api_reader.get_deliveries(next_delivery_ts_str, sort_field, sort_direction, limit, offset)
                self.log.info(f'Found {len(load_queue)} documents to sync from api.')
                if not load_queue:
                    self.log.info('Finishing work. Quitting.')
                    break
                else:
                    offset += limit

                for i, d in enumerate(load_queue):
                    self.pg_saver.save_delivery(conn, d)

                max_delivery_ts_str = max(
                    [
                        datetime.strptime(
                            t['delivery_ts'],
                            '%Y-%m-%d %H:%M:%S.%f' if '.' in t['delivery_ts'] else '%Y-%m-%d %H:%M:%S'
                        ) for t in load_queue
                    ]
                ).strftime('%Y-%m-%d %H:%M:%S')

            wf_setting.workflow_settings[last_delivery_ts_key] = max_delivery_ts_str
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f'Finishing work. Last checkpoint: {wf_setting_json}')

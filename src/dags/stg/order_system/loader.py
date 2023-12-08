from datetime import datetime
from logging import Logger, getLogger

from airflow.models import Variable
from psycopg import Connection
from typing import Any, List, Dict

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.dict_util import json2str
from lib import MongoConnect, ConnectionBuilder, PgConnect


class MongoReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_objects(self, object_name: str, load_threshold: datetime) -> List[Dict]:
        return list(
            self.dbs.get_collection(object_name)
            .find(filter={'update_ts': {'$gt': load_threshold}}, sort=[('update_ts', 1)])
        )


class PgSaver:

    def save_object(self, conn: Connection, object_name:str, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                f'''
                    INSERT INTO stg.ordersystem_{object_name} (object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                ''',
                {
                    'id': id,
                    'val': str_val,
                    'update_ts': update_ts
                }
            )


class Loader:
    LAST_LOADED_TS_KEY = 'last_loaded_ts'

    def __init__(
        self, 
        table_name: str, 
        mc: MongoConnect = None, 
        pg_dest: PgConnect = None,
        logger: Logger = None
    ) -> None:
        cert_path = Variable.get('MONGO_DB_CERTIFICATE_PATH')
        db_user = Variable.get('MONGO_DB_USER')
        db_pw = Variable.get('MONGO_DB_PASSWORD')
        rs = Variable.get('MONGO_DB_REPLICA_SET')
        db = Variable.get('MONGO_DB_DATABASE_NAME')
        host = Variable.get('MONGO_DB_HOST')

        self.table_name = table_name
        self.wf_key = f'order_system_{table_name}'
        self.collection_loader = MongoReader(mc or MongoConnect(cert_path, db_user, db_pw, host, rs, db, db))
        self.pg_saver = PgSaver()
        self.pg_dest = pg_dest or ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger or getLogger(__name__)

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.wf_key)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f'starting to load from last checkpoint: {last_loaded_ts}')

            load_queue = self.collection_loader.get_objects(self.table_name, last_loaded_ts)
            self.log.info(f'Found {len(load_queue)} documents to sync from collection.')
            if not load_queue:
                self.log.info('Quitting.')
                return 0

            for i, d in enumerate(load_queue):
                self.pg_saver.save_object(conn, self.table_name, str(d['_id']), d['update_ts'], d)
                self.log.info(f'processed {i} documents of {len(load_queue)} while syncing restaurants.')

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t['update_ts'] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f'Finishing work. Last checkpoint: {wf_setting_json}')

            return len(load_queue)

from datetime import datetime
from logging import Logger, getLogger
from typing import List, Any

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect, ConnectionBuilder
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DefaultOriginRepository:
    def __init__(self, pg: PgConnect = None) -> None:
        self._db = pg or ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')

    def list_objects(self, object_threshold: int) -> List:
        pass


class DefaultDestRepository:

    def insert_object(self, conn: Connection, object_to_insert: Any) -> None:
        pass


class DefaultLoader:
    WF_KEY = 'default'
    origin_class = DefaultOriginRepository
    dest_class = DefaultDestRepository
    LAST_LOADED_ID_KEY = 'last_loaded_id'

    def __init__(self, pg_origin: PgConnect = None, pg_dest: PgConnect = None, log: Logger = None) -> None:
        self.pg_dest = pg_dest or ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')
        self.origin = self.origin_class(pg_origin or ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION'))
        self.stg = self.dest_class()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log or getLogger(__name__)

    def load_objects(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_objects(last_loaded)
            self.log.info(f'Found {len(load_queue)} objects to load.')
            if not load_queue:
                self.log.info('Quitting.')
                return

            for object_to_insert in load_queue:
                self.stg.insert_object(conn, object_to_insert)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f'Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}')


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository(DefaultOriginRepository):

    def list_objects(self, event_threshold: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                '''
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                ''', {
                    'threshold': event_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class EventDestRepository(DefaultDestRepository):

    def insert_object(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                '''
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                ''',
                {
                    'id': event.id,
                    'event_ts': event.event_ts,
                    'event_type': event.event_type,
                    'event_value': event.event_value
                },
            )


class EventLoader(DefaultLoader):
    WF_KEY = 'bonus_system_events'
    origin_class = EventsOriginRepository
    dest_class = EventDestRepository


class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


class RanksOriginRepository(DefaultOriginRepository):

    def list_objects(self, rank_threshold: int) -> List[RankObj]:
        with self._db.client().cursor(row_factory=class_row(RankObj)) as cur:
            cur.execute(
                '''
                    SELECT id, name, bonus_percent, min_payment_threshold
                    FROM ranks
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                ''', {
                    'threshold': rank_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class RankDestRepository(DefaultDestRepository):

    def insert_object(self, conn: Connection, rank: RankObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                '''
                    INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                    VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold;
                ''',
                {
                    'id': rank.id,
                    'name': rank.name,
                    'bonus_percent': rank.bonus_percent,
                    'min_payment_threshold': rank.min_payment_threshold
                },
            )


class RankLoader(DefaultLoader):
    WF_KEY = 'bonus_system_ranks'
    origin_class = RanksOriginRepository
    dest_class = RankDestRepository


class UserObj(BaseModel):
    id: int
    order_user_id: str


class UsersOriginRepository(DefaultOriginRepository):

    def list_objects(self, user_threshold: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                '''
                    SELECT id, order_user_id
                    FROM users
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                ''', {
                    'threshold': user_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class UserDestRepository(DefaultDestRepository):

    def insert_object(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                '''
                    INSERT INTO stg.bonussystem_users(id, order_user_id)
                    VALUES (%(id)s, %(order_user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_user_id = EXCLUDED.order_user_id;
                ''',
                {
                    'id': user.id,
                    'order_user_id': user.order_user_id
                },
            )


class UserLoader(DefaultLoader):
    WF_KEY = 'bonus_system_users'
    origin_class = UsersOriginRepository
    dest_class = UserDestRepository

import logging
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder, PgConnect

log = logging.getLogger(__name__)


class SchemaDdl:
    def __init__(self, pg: PgConnect, log: logging.Logger) -> None:
        self._db = pg
        self.log = log

    def init_schema(self, path_to_scripts: Path) -> None:

        for i, file in enumerate(path_to_scripts.iterdir()):
            self.log.info(f'Iteration {i}. Applying file {file.name}')
            script = file.read_text()

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

            self.log.info(f'Iteration {i}. File {file.name} executed successfully.')


@dag(
    schedule_interval='@once',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'schema', 'ddl'],
    is_paused_upon_creation=False
)
def init_schema():
    dwh_pg_connect = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')
    ddl_path = Path(__file__).parent.joinpath('ddl')

    @task
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    schema_init()


init_schema = init_schema()

from pathlib import Path

import pendulum
from airflow.decorators import dag, task

from lib import TableBuilder


@dag(
    schedule_interval='30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'cdm'],
    is_paused_upon_creation=False
)
def cdm():
    sql_path = Path(__file__).parent.joinpath('sql')

    @task()
    def settlement_report():
        table_builder = TableBuilder(sql_path)
        table_builder.build_table('dm_settlement_report')

    settlement_report()


dds_dag = cdm()

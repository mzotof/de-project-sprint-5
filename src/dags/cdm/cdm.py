from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from lib import TableBuilder


@dag(
    schedule_interval='0,30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'cdm'],
    is_paused_upon_creation=False
)
def cdm():
    sql_path = Path(__file__).parent.joinpath('sql')

    dds_sensor = ExternalTaskSensor(
        task_id='dds_sensor',
        external_dag_id='dds',
        execution_delta=timedelta(minutes=15),
        poke_interval=60,
        timeout=3600,
        mode='reschedule',
    )

    @task
    def settlement_report():
        table_builder = TableBuilder(sql_path)
        table_builder.build_table('dm_settlement_report')

    settlement_report_task = settlement_report()

    def courier_ledger(current_date: str):
        table_builder = TableBuilder(sql_path)
        table_builder.build_table('dm_courier_ledger', current_date)

    courier_ledger_task = PythonOperator(
        task_id='courier_ledger',
        python_callable=courier_ledger,
        op_kwargs={'current_date': '{{ ds }}'},
    )

    dds_sensor >> settlement_report_task >> courier_ledger_task


dds_dag = cdm()

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from lib import TableBuilder


@dag(
    schedule_interval='15,45 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'dds'],
    is_paused_upon_creation=False
)
def dds():
    sql_path = Path(__file__).parent.joinpath('sql')
    table_builder = TableBuilder(sql_path)

    with TaskGroup('sensors') as sensors_tasks:
        default_kwargs = {
            'execution_delta': timedelta(minutes=15),
            'poke_interval': 60,
            'timeout': 3600,
            'mode': 'reschedule',
        }
        ExternalTaskSensor(task_id='bonus_system_sensor', external_dag_id='stg_bonus_system', **default_kwargs)
        ExternalTaskSensor(task_id='courier_system_sensor', external_dag_id='stg_courier_system', **default_kwargs)
        ExternalTaskSensor(task_id='order_system_sensor', external_dag_id='stg_order_system', **default_kwargs)

    with TaskGroup('build_dds') as build_dds_tasks:
        @task()
        def users():
            table_builder.build_table('dm_users')

        users_task = users()

        @task()
        def restaurants():
            table_builder.build_table('dm_restaurants')

        restaurants_task = restaurants()

        @task()
        def timestamps():
            table_builder.build_table('dm_timestamps')

        timestamps_task = timestamps()

        @task()
        def products():
            table_builder.build_table('dm_products')

        products_task = products()

        @task()
        def orders():
            table_builder.build_table('dm_orders')

        orders_task = orders()

        @task()
        def fct_product_sales():
            table_builder.build_table('fct_product_sales')

        fct_product_sales_task = fct_product_sales()

        @task()
        def couriers():
            table_builder.build_table('dm_couriers')

        couriers_task = couriers()

        @task()
        def deliveries():
            table_builder.build_table('dm_deliveries')

        deliveries_task = deliveries()

        restaurants_task >> products_task
        [users_task, restaurants_task, timestamps_task] >> orders_task
        [orders_task, products_task] >> fct_product_sales_task
        [orders_task, couriers_task, timestamps_task] >> deliveries_task

    sensors_tasks >> build_dds_tasks

dds_dag = dds()

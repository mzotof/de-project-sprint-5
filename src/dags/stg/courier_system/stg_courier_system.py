import pendulum
from airflow.decorators import dag, task
from stg.courier_system.loader import Loader


@dag(
    schedule_interval='0,30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_courier_system():
    loader = Loader()

    @task
    def load_couriers():
        loader.run_copy_couriers()

    load_couriers()

    @task
    def load_deliveries():
        loader.run_copy_deliveries()

    load_deliveries()


stg_courier_system_dag = stg_courier_system()

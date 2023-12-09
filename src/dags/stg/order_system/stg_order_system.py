import pendulum
from airflow.decorators import dag, task
from stg.order_system.loader import Loader


@dag(
    schedule_interval='0,30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_order_system():

    @task()
    def load_restaurants():
        loader = Loader('restaurants')
        loader.run_copy()

    load_restaurants()

    @task()
    def load_users():
        loader = Loader('users')
        loader.run_copy()

    load_users()

    @task()
    def load_orders():
        loader = Loader('orders')
        loader.run_copy()

    load_orders()


stg_order_system_dag = stg_order_system()

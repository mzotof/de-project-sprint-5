import pendulum
from airflow.decorators import dag, task
from stg.bonus_system.loaders import RankLoader, UserLoader, EventLoader


@dag(
    schedule_interval='0,30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_bonus_system():

    @task
    def load_ranks():
        rest_loader = RankLoader()
        rest_loader.load_objects()

    load_ranks()

    @task
    def load_users():
        rest_loader = UserLoader()
        rest_loader.load_objects()

    load_users()

    @task
    def load_events():
        rest_loader = EventLoader()
        rest_loader.load_objects()

    load_events()


stg_bonus_system_dag = stg_bonus_system()

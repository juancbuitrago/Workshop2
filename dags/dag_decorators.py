# dag_decorators.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from etl import extract_spotify, extract_and_load_grammys, transform, load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),  # Adjust the start date as needed
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(default_args=default_args, schedule_interval='@daily', tags=['spotify', 'grammys'])
def etl_dag():
    
    @task
    def extract_spotify_task():
        return extract_spotify()

    @task
    def extract_and_load_grammys_task():
        extract_and_load_grammys()
        return 'Grammys data loaded'

    @task
    def transform_task(data):
        return transform(data)

    @task
    def load_task(data):
        load(data)

    spotify_data = extract_spotify_task()
    transform_spotify_data = transform_task(spotify_data)
    load_spotify_data = load_task(transform_spotify_data)
    load_grammys_data = extract_and_load_grammys_task()

etl_workflow = etl_dag()
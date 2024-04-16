# dag_decorators.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from etl import extract, transform, load

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

@dag(default_args=default_args, description='ETL DAG for Spotify data', schedule_interval='@daily', tags=['spotify'])
def spotify_etl_dag():
    
    @task
    def extract_task():
        return extract()

    @task
    def transform_task(spotify_df):
        return transform(spotify_df)

    @task
    def load_task(spotify_df):
        load(spotify_df)

    # Define the workflow
    spotify_data = extract_task()
    transformed_data = transform_task(spotify_data)
    load_task(transformed_data)

# Instantiate the DAG
spotify_etl_workflow = spotify_etl_dag()

# dag_decorators.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from etl import (extract_spotify, load_grammys_to_db, extract_grammys,
                transform_spotify, transform_grammys, merge_datasets, load_to_db, upload_to_drive)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(default_args=default_args, description='ETL DAG for merging Spotify and Grammy data', schedule_interval='@daily', tags=['data merging'])
def data_merging_etl_dag():
    
    @task
    def extract_spotify_task():
        return extract_spotify()

    @task
    def load_and_extract_grammys_task():
        load_grammys_to_db()
        return extract_grammys()

    @task
    def transform_spotify_task(spotify_df):
        return transform_spotify(spotify_df)

    @task
    def transform_grammys_task(grammy_df):
        return transform_grammys(grammy_df)

    @task
    def merge_datasets_task(spotify_df, grammy_df):
        return merge_datasets(spotify_df, grammy_df)

    @task
    def load_to_db_task(final_merged_df):
        table_name = 'merged_data'
        load_to_db(final_merged_df, table_name)

    @task
    def upload_to_gdrive_task(final_merged_df):
        filename = 'merged_data.csv'
        gdrive_folder_id = '1hYJ-2qWM1UVras0vtUKxLzG7OCiYztC2'  
        upload_to_drive(final_merged_df, filename, gdrive_folder_id)

    spotify_data = extract_spotify_task()
    grammy_data = load_and_extract_grammys_task()
    transformed_spotify_data = transform_spotify_task(spotify_data)
    transformed_grammy_data = transform_grammys_task(grammy_data)
    merged_data = merge_datasets_task(transformed_spotify_data, transformed_grammy_data)
    load_to_db_task(merged_data)
    upload_to_gdrive_task(merged_data)  

spotify_grammy_etl_workflow = data_merging_etl_dag()

# etl.py
import json
import logging
import pandas as pd
from sqlalchemy import create_engine


def extract_spotify():
    spotify_df = pd.read_csv('/opt/airflow/data/spotify_dataset.csv')
    logging.info("Spotify data extraction finished")
    return spotify_df


def extract_and_load_grammys():
    with open('/opt/airflow/config/config.json', 'r') as file:
        config = json.load(file)
    
    engine = create_engine(f'postgresql+pg8000://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}')
    
    grammy_df = pd.read_csv('/opt/airflow/data/the_grammy_awards.csv')
    
    grammy_df.to_sql('grammys', engine, index=False, if_exists='replace')
    
    logging.info("Grammy data successfully inserted into database.")

def transform(spotify_df):

    columns_to_drop = [
        'Unnamed: 0',
        'track_id',
        'key',
        'mode',
        'instrumentalness',
        'time_signature',
        'liveness',
        'valence'
    ]
    spotify_df.drop(columns=columns_to_drop, axis=1, inplace=True)
    logging.info("Transformations applied to Spotify data")
    return spotify_df

def load(spotify_df):
    spotify_df.to_csv('/opt/airflow/outputs/spotify.csv', index=False)
    logging.info("Spotify data loaded into the output file")

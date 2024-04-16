# etl.py
import pandas as pd
import logging

def extract():
    spotify_df = pd.read_csv('/opt/airflow/data/spotify_dataset.csv')
    logging.info("Spotify data extraction finished")
    return spotify_df

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

# etl.py
import json
import logging
import pandas as pd
from sqlalchemy import create_engine


def extract_spotify():
    spotify_df = pd.read_csv('/opt/airflow/data/spotify_dataset.csv')
    logging.info("Spotify data extraction finished")
    return spotify_df


def load_grammys_to_db():
    with open('/opt/airflow/config/config.json', 'r') as file:
        config = json.load(file)
    
    engine = create_engine(f'postgresql+pg8000://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}')
    
    grammy_df = pd.read_csv('/opt/airflow/data/the_grammy_awards.csv')
    
    grammy_df.to_sql('grammys', engine, index=False, if_exists='replace')
    
    logging.info("Grammy data successfully inserted into database.")
def extract_grammys():
    with open('/opt/airflow/config/config.json', 'r') as file:
        config = json.load(file)
    
    engine = create_engine(f'postgresql+pg8000://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}')
    grammy_df = pd.read_sql_table('grammys', engine)
    logging.info("Grammy data extraction finished")
    return grammy_df

def transform_spotify(spotify_df):
    # Drop unnecessary columns
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
    
    spotify_df['artists'] = spotify_df['artists'].str.lower().str.split(';').str[0]
    spotify_df['track_name'] = spotify_df['track_name'].str.lower().str.replace(r"\(.*\)", "", regex=True)
    
    logging.info("Transformations applied to Spotify data")
    return spotify_df

def transform_grammys(grammy_df):

    grammy_df['artist'] = grammy_df['artist'].str.lower()
    grammy_df['nominee'] = grammy_df['nominee'].str.lower().str.replace(r"\(.*\)", "", regex=True)
    
    columns_to_drop_grammys = ['updated_at', 'published_at', 'workers', 'img']
    grammy_df.drop(columns=columns_to_drop_grammys, axis=1, inplace=True)

    logging.info("Transformations applied to Grammy data")
    return grammy_df

def merge_datasets(spotify_df, grammy_df):
    primary_merge = pd.merge(spotify_df, grammy_df, left_on='artists', right_on='artist', how='inner')
    secondary_merge = pd.merge(spotify_df, grammy_df, left_on='track_name', right_on='nominee', how='inner')
    
    final_merged_df = pd.concat([primary_merge, secondary_merge]).drop_duplicates()
    
    logging.info(f"Merged dataset created with {final_merged_df.shape[0]} entries")
    return final_merged_df

def load_to_db(final_merged_df, table_name):
    with open('/opt/airflow/config/config.json', 'r') as file:
        config = json.load(file)

    conn_string = f'postgresql+pg8000://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}'
    engine = create_engine(conn_string)
    
    final_merged_df.to_sql(table_name, engine, index=False, if_exists='replace')
    logging.info(f"Merged data loaded into the database table {table_name}")

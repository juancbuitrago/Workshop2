# etl.py
import json
import logging
import pandas as pd
from io import StringIO
from io import BytesIO  
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from airflow.exceptions import AirflowException
import os


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
    genre_categories = {
        'Rock': ['alt-rock', 'grunge', 'hard-rock', 'punk-rock', 'rock', 'rock-n-roll', 'goth', 'punk', 'psych-rock', 'j-rock', 'acoustic', 'brittish', 'rockabilly'],
        'Pop': ['pop', 'power-pop', 'pop-film', 'k-pop', 'j-pop', 'cantopop', 'children', 'disney', 'happy', 'kids', 'mandopop', 'mpb', 'power-pop', 'romance', 'sad', 'singer-songwriter', 'spanish'],
        'Electronic/Dance': ['electronic', 'dubstep', 'edm', 'electro', 'techno', 'trance', 'house', 'deep-house', 'disco', 'dancehall', 'chicago-house', 'detroit-techno', 'hardstyle', 'minimal-techno', 'j-dance', 'party', 'breakbeat', 'drum-and-bass', 'dub', 'progressive-house', 'trip-hop'],
        'Hip-Hop/R&B': ['hip-hop', 'r-n-b', 'j-idol', 'afrobeat'],
        'Metal': ['black-metal', 'death-metal', 'heavy-metal', 'metal', 'metalcore', 'grindcore', 'industrial', 'hardcore'],
        'Jazz/Blues': ['jazz', 'blues'],
        'Folk/Country': ['folk', 'country', 'bluegrass', 'forro', 'honky-tonk'],
        'Latin': ['latin', 'salsa', 'samba', 'reggaeton', 'latino'],
        'Classical/Opera': ['classical', 'opera', 'piano'],
        'Indie/Alternative': ['alternative', 'indie', 'indie-pop', 'singer-songwriter', 'emo', 'ska'],
        'World Music': ['world-music', 'brazil', 'indian', 'iranian', 'malay', 'mandopop', 'swedish', 'turkish', 'french', 'german', 'reggae', 'synth-pop'],
        'Ambient/Chill/Downtempo': ['ambient', 'chill', 'new-age', 'sleep', 'tango', 'study'],
        'Funk/Soul': ['funk', 'soul', 'gospel', 'groove']
    }
    
    spotify_df['genre_category'] = 'Other'  
    
    
    for category, genres in genre_categories.items():
        spotify_df.loc[spotify_df['track_genre'].isin(genres), 'genre_category'] = category

    spotify_df.drop_duplicates(subset=['track_id'], keep='first', inplace=True)
    spotify_df = spotify_df.dropna(subset=['artists'])
    spotify_df['num_artists'] = spotify_df['artists'].str.split(';').apply(len)
    spotify_df['second_artist'] = spotify_df['artists'].apply(lambda x: x.split(';')[1] if len(x.split(';')) > 1 else "No second artist")
    spotify_df['popularity_category'] = pd.cut(spotify_df['popularity'], bins=[0, 33, 66, 100], labels=['Low', 'Medium', 'High'])

    
    columns_to_drop = [
        'Unnamed: 0',
        'track_id',
        'key',
        'mode',
        'instrumentalness',
        'time_signature',
        'liveness',
        'valence',
    
    ]
    spotify_df.drop(columns=columns_to_drop, axis=1, inplace=True)
    spotify_df.drop_duplicates(inplace=True)

    

    logging.info("Transformations applied to Spotify data")
    return spotify_df




def transform_grammys(grammy_df):


    grammy_df['artist'] = grammy_df['artist'].fillna(grammy_df['workers']).str.extract(r'([^\+]+)')[0]

    grammy_df['workers'] = grammy_df['workers'].fillna(grammy_df['artist'])

    grammy_df['category'] = grammy_df['category'].str.replace(r'\[|\]', '', regex=True)

    mask = grammy_df['category'].str.contains('Artist', case=False, na=False)
    grammy_df.loc[mask, ['artist', 'workers']] = grammy_df['nominee']

    grammy_df = grammy_df.drop(columns=['published_at', 'updated_at', 'img'])

    logging.info("Transformations applied to Grammy data")
    return grammy_df


def merge_datasets(spotify_df, grammy_df):
    final_merged_df = pd.merge(spotify_df, 
                               grammy_df, 
                               left_on='track_name', 
                               right_on='nominee', 
                               how='left'
                              )
    
    final_merged_df.drop(columns=['workers', 'artist', 'year'], inplace=True)
    
    columns_to_fill = ['title', 'category', 'nominee']
    fill_value = 'not nominated'
    final_merged_df[columns_to_fill] = final_merged_df[columns_to_fill].fillna(fill_value)
    
    final_merged_df['winner'] = final_merged_df['winner'].fillna(False)
    
    
    logging.info(f"Merged dataset created with {final_merged_df.shape[0]} entries")
    
    return final_merged_df

def load_to_db(final_merged_df, table_name):
    with open('/opt/airflow/config/config.json', 'r') as file:
        config = json.load(file)

    conn_string = f'postgresql+pg8000://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}'
    engine = create_engine(conn_string)
    
    final_merged_df.to_sql(table_name, engine, index=False, if_exists='replace')
    logging.info(f"Merged data loaded into the database table {table_name}")

def upload_to_drive(df, filename, folder_id):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = '/opt/airflow/config/service_account.json'

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    csv_str = df.to_csv(index=False)
    csv_bytes = csv_str.encode('utf-8')  
    csv_io = BytesIO(csv_bytes)  

    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaIoBaseUpload(csv_io, mimetype='text/csv', resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    logging.info(f"Uploaded file with ID: {file.get('id')}")
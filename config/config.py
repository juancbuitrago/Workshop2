import json
import pandas as pd
from sqlalchemy import create_engine


def db_connection():
    with open('../config/config.json', 'r') as file:
        config = json.load(file)
    
    engine = create_engine(f'postgresql+pg8000://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}')

    return engine


def insert_data(engine, csv_file_path, table_name):

    data = pd.read_csv(csv_file_path)
    
    data.to_sql(table_name, engine, index=False, if_exists='replace')
    
    print(f"Data successfully inserted into {table_name}.")

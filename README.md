# Workshop2# ETL Workshop 1 #
## Structure ##
<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        .
        ├── .gitignore
        ├── docker-compose.yaml
        ├── Dockerfile
        ├── README.md
        ├── requirements.txt
        ├── config
        │   ├── config_EDA.json
        │   ├── config.json
        │   └── service_account.json
        ├── dags
        │   ├── dag.py
        │   └── etl.py
        ├── data
        │   ├── spotify_dataset.csv
        │   └── the_grammy_awards.csv
        ├── docs
        │   ├── dashboard.pdf
        │   └── documentation.pdf
        └── notebooks
            ├── EDA_001.ipynb
            └── EDA_002.ipynb
        
</div>

## Overview ##
_This workshop is an exercise on how to build an ETL pipeline using Apache Airflow, the idea is to extract information using two different data sources (csv file, database), then do some transformations and merge the transformed data to finally load into google drive as a CSV file and store the data in a DB. As a last step, create a dashboard from the data stored in the DB to visualize the information._

_Also, *[Detailed Documentation](https://github.com/juancbuitrago/Workshop2/blob/main/docs/documentation.pdf)* is available that covers everything from data selection to the final visualizations_

## Table of Contents ##
- [Requirements](#requirements)
- [Setup](#setup)
- [Data Transformation](#data-transformation)
- [Data Analysis](#exploratory-data-analysis)
- [Analysis & Visualizations](#analysis-visualizations)

## Requirements <a name="requirements"></a> ##
- Create a folder and clone this github repository
- Python 3.9
- Airflow
- Docker
- PostgreSQL
In this section you need to create a database called grammys (or you can renamed it but you should change the name of the database in the rest of the code.)
- Jupyter Notebook
- JSON credentials file ("config_EDA.json") with this format:
 
```
{
    "user": your user,
    "password": your password,
    "host": "localhost",
    "dbname": your db name,
    "port": your port
}

``` 
- JSON credentials file for docker works in localhost("config.json") with this format:
``` 
{
    "user": your user,
    "password": your password,
    "host": "host.docker.internal", <- This is really important
    "dbname": your db name,
    "port": your port
}
``` 
- JSON credentials file for drive connection with Google cloud("service_account.json") you can get this file from the Google cloud api in service account section after you create your service account.

- Libraries that we install with the requirements.txt (This process makes him automatically when you run the docker-compose up):
    - pandas
    - matplotlib
    - numpy
    - SQLalchemy
    - psycopg2
    - seaborn

## Setup <a name="setup"></a> ##
_First of all, 
ensure you have the following programs installed with which the entire project procedure is carried out:_

   - **[Python](https://www.python.org)**
   - **[PostgreSQL](https://www.postgresql.org/download/)**
   - **[PowerBI](https://powerbi.microsoft.com/es-es/downloads/)**
   - **[VS Code](https://code.visualstudio.com/download)** or **[Jupyter](https://jupyter.org/install)**
   - **[Docker Desktop](https://www.docker.com/products/docker-desktop/)**

If you already have all the requirements, you only need to run Docker Desktop and then:
1. Go to the root of the repository
2. Run `docker-compose up airflow-init`
3. Run `docker-compose up`
4. Go to: http://localhost:8080/
5. Log in with the credentials: airflow airflow
6. Run the dag: data_merging_etl_dag

## Grammys EDA <a name="data-transformation"></a> ##

 _This process was carried out in **[EDA_001](https://github.com/juancbuitrago/Workshop2/blob/main/notebooks/EDA_001.ipynb)** where the following procedures are being carried out:_

- Identification of the data frame structure
- Identification of columns names
- Identification of data types
- Identification of null data
- Identification of unnecesary columns
- Identification of problems in the data
- Example of transformations
- Exploratory Data Analysis
- Graphics
 
 ## Spotify EDA <a name="exploratory-data-analysis"></a> ##

 _This process was carried out in **[EDA_002](https://github.com/juancbuitrago/Workshop2/blob/main/notebooks/EDA_002.ipynb)** where the following procedures are being carried out:_

- Identification of the data frame structure
- Identification of columns names
- Identification of data types
- Identification of null data
- Identification of duplicated values
- Identification of unnecesary columns
- Identification of problems in the data
- Example of transformations
- Exploratory Data Analysis
- Graphics
## Analysis & Visualization of the merged data <a name="analysis-visualizations"></a> ###

### These visualizations can be seen in the **[Dashboard of merged data](https://github.com/juancbuitrago/Workshop2/blob/main/docs/dashboard.pdf)**.

# _[Clone me](https://github.com/juancbuitrago/Workshop1.git) and see how powerful the data can be!_
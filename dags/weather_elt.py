import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from google.cloud import bigquery


PROJECT_ID = 'valid-arc-437913-k1'
DATASET_ID = 'weather_etl_pipeline'
CITIES = ['London', 'New York', 'Paris', 'Tokyo', 'Sydney']
API_KEY ="1a13bf4cca15e0fd022e4cc052c15332"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Bigquery
CREATE_CITIES_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.cities` (
    city_id INT64,
    city_name STRING,
    country_code STRING,
    latitude FLOAT64,
    longitude FLOAT64
) PARTITION BY DATE(_PARTITIONTIME);
"""

CREATE_WEATHER_CONDITIONS_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.weather_conditions` (
    condition_id INT64,
    condition_main STRING,
    condition_description STRING,
    icon_code STRING
) PARTITION BY DATE(_PARTITIONTIME);
"""

CREATE_MEASUREMENTS_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.measurements` (
    measurement_id INT64,
    city_id INT64,
    condition_id INT64,
    temperature FLOAT64,
    feels_like FLOAT64,
    humidity INT64,
    pressure INT64,
    wind_speed FLOAT64,
    timestamp TIMESTAMP,
    FOREIGN KEY (city_id) REFERENCES cities(city_id),
    FOREIGN KEY (condition_id) REFERENCES weather_conditions(condition_id)
) PARTITION BY DATE(timestamp);
"""

def fetch_weather_data(**context):
    base_url = 'http://api.openweathermap.org/data/2.5/weather'
    weather_data = []
    
    for city in CITIES:
        params = {
            'q': city,
            'appid': API_KEY,
            'units': 'metric'
        }
        response = requests.get(base_url, params=params)
        data = response.json()
        weather_data.append(data)
    
    return weather_data

def normalize_weather_data(**context):
    ti = context['task_instance']
    raw_data = ti.xcom_pull(task_ids='fetch_weather_data')
    
    # Prepare normalized dataframes
    cities_data = []
    conditions_data = []
    measurements_data = []
    
    for data in raw_data:
        # Cities table
        city_data = {
            'city_id': data['id'],
            'city_name': data['name'],
            'country_code': data['sys']['country'],
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon']
        }
        cities_data.append(city_data)
        
        # Weather conditions table
        condition = data['weather'][0]
        condition_data = {
            'condition_id': condition['id'],
            'condition_main': condition['main'],
            'condition_description': condition['description'],
            'icon_code': condition['icon']
        }
        conditions_data.append(condition_data)
        
        # Measurements table
        measurement_data = {
            'measurement_id': int(f"{data['id']}{int(datetime.now().timestamp())}"),
            'city_id': data['id'],
            'condition_id': condition['id'],
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'timestamp': datetime.utcnow().isoformat()
        }
        measurements_data.append(measurement_data)
    
    # Convert to DataFrames
    cities_df = pd.DataFrame(cities_data).drop_duplicates()
    conditions_df = pd.DataFrame(conditions_data).drop_duplicates()
    measurements_df = pd.DataFrame(measurements_data)
    
    # Store DataFrames in BigQuery
    cities_df.to_gbq(
        f'{DATASET_ID}.cities',
        project_id=PROJECT_ID,
        if_exists='append'
    )
    
    conditions_df.to_gbq(
        f'{DATASET_ID}.weather_conditions',
        project_id=PROJECT_ID,
        if_exists='append'
    )
    
    measurements_df.to_gbq(
        f'{DATASET_ID}.measurements',
        project_id=PROJECT_ID,
        if_exists='append'
    )

# Create DAG
dag = DAG(
    'weather_bigquery_etl',
    default_args=default_args,
    description='A DAG to fetch weather data and store in BigQuery using normalized schema',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# Create tables tasks
create_cities_table = BigQueryInsertJobOperator(
    task_id='create_cities_table',
    configuration={
        'query': {
            'query': CREATE_CITIES_TABLE,
            'useLegacySql': False
        }
    },
    dag=dag
)

create_conditions_table = BigQueryInsertJobOperator(
    task_id='create_conditions_table',
    configuration={
        'query': {
            'query': CREATE_WEATHER_CONDITIONS_TABLE,
            'useLegacySql': False
        }
    },
    dag=dag
)

create_measurements_table = BigQueryInsertJobOperator(
    task_id='create_measurements_table',
    configuration={
        'query': {
            'query': CREATE_MEASUREMENTS_TABLE,
            'useLegacySql': False
        }
    },
    dag=dag
)

# Fetch and process data tasks
fetch_data = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)

process_data = PythonOperator(
    task_id='normalize_weather_data',
    python_callable=normalize_weather_data,
    dag=dag
)

# Set task dependencies
[create_cities_table, create_conditions_table] >> create_measurements_table >> fetch_data >> process_data
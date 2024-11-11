import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import requests
from google.oauth2 import service_account
from google.cloud import bigquery
import json
import uuid
from google.cloud import bigquery
from typing import Dict, List

# Configuration
OPENWEATHER_API_KEY = '1a13bf4cca15e0fd022e4cc052c15332'
CITIES = ['London', 'New York', 'Tokyo', 'Paris', 'Sydney']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'
PROJECT_ID = 'valid-arc-437913-k1'
DATASET = 'weather_dataset'
#key_path = "C:/Users/USER/Downloads/valid-arc-437913-k1-f5a82d66ffc6.json"
#credentials = service_account.Credentials.from_service_account_file(key_path)
#bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather_data(**context) -> Dict:
    """Fetch weather data for multiple cities."""
    all_data = []
    
    for city in CITIES:
        params = {
            'q': city,
            'appid': OPENWEATHER_API_KEY,
            'units': 'metric'
        }
        
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        all_data.append(response.json())
    
    return {'weather_data': all_data}

def normalize_weather_data(**context) -> Dict:
    """Normalize weather data into separate tables."""
    ti = context['task_instance']
    raw_data = ti.xcom_pull(task_ids='fetch_weather')['weather_data']
    
    cities = []
    conditions = []
    measurements = []
    
    for data in raw_data:
        # Cities table
        city_id = str(data['id'])
        cities.append({
            'city_id': city_id,
            'name': data['name'],
            'country': data['sys']['country'],
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon']
        })
        
        # Weather conditions table
        condition_id = str(data['weather'][0]['id'])
        conditions.append({
            'condition_id': condition_id,
            'main': data['weather'][0]['main'],
            'description': data['weather'][0]['description'],
            'icon': data['weather'][0]['icon']
        })
        
        # Measurements table
        measurements.append({
            'measurement_id': str(uuid.uuid4()),
            'city_id': city_id,
            'condition_id': condition_id,
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'wind_direction': data['wind'].get('deg', 0),
            'measurement_time': datetime.utcfromtimestamp(data['dt']).isoformat()
        })
    
    return {
        'cities': cities,
        'conditions': conditions,
        'measurements': measurements
    }

# Create DAG
with DAG(
    'weather_etl_dag_2024',
    default_args=default_args,
    description='ETL DAG for weather data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather'],
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
    )

    normalize_data = PythonOperator(
        task_id='normalize_data',
        python_callable=normalize_weather_data,
    )

    # Insert data into BigQuery tables
    load_cities = BigQueryInsertJobOperator(
        task_id='load_cities',
        configuration={
            'query': {
                'query': """
                MERGE `{{ params.project_id }}.{{ params.dataset }}.cities` T
                USING UNNEST({{ task_instance.xcom_pull(task_ids='normalize_data')['cities'] }}) S
                ON T.city_id = S.city_id
                WHEN NOT MATCHED THEN
                    INSERT (city_id, name, country, latitude, longitude)
                    VALUES (city_id, name, country, latitude, longitude)
                """,
                'useLegacySql': False,
            }
        },
        params={'project_id': PROJECT_ID, 'dataset': DATASET},
    )

    load_conditions = BigQueryInsertJobOperator(
        task_id='load_conditions',
        configuration={
            'query': {
                'query': """
                MERGE `{{ params.project_id }}.{{ params.dataset }}.weather_conditions` T
                USING UNNEST({{ task_instance.xcom_pull(task_ids='normalize_data')['conditions'] }}) S
                ON T.condition_id = S.condition_id
                WHEN NOT MATCHED THEN
                    INSERT (condition_id, main, description, icon)
                    VALUES (condition_id, main, description, icon)
                """,
                'useLegacySql': False,
            }
        },
        params={'project_id': PROJECT_ID, 'dataset': DATASET},
    )

    load_measurements = BigQueryInsertJobOperator(
        task_id='load_measurements',
        configuration={
            'query': {
                'query': """
                INSERT INTO `{{ params.project_id }}.{{ params.dataset }}.weather_measurements`
                SELECT * FROM UNNEST({{ task_instance.xcom_pull(task_ids='normalize_data')['measurements'] }})
                """,
                'useLegacySql': False,
            }
        },
        params={'project_id': PROJECT_ID, 'dataset': DATASET},
    )

    # Set up task dependencies
    fetch_weather >> normalize_data >> [load_cities, load_conditions, load_measurements]
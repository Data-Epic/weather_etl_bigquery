from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import logging


CITIES = ['London', 'New York', 'Tokyo', 'Sydney', 'Paris']
API_KEY = '1a13bf4cca15e0fd022e4cc052c15332'  # Set this in Airflow Variables
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def fetch_weather_data(**context):
    weather_data = []
    for city in CITIES:
        try:
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'
            }
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            weather_data.append({
                'city': city,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'weather_description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'timestamp': datetime.utcnow()
            })
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {city}: {str(e)}")
            continue
            
    context['task_instance'].xcom_push(key='weather_data', value=weather_data)

def store_weather_data(**context):
    weather_data = context['task_instance'].xcom_pull(task_ids='fetch_weather', key='weather_data')
    
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    
    cur = conn.cursor()
    
    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            humidity INTEGER,
            pressure INTEGER,
            weather_description TEXT,
            wind_speed FLOAT,
            timestamp TIMESTAMP
        )
    """)
    
    # Prepare data for batch insert
    values = [(
        d['city'],
        d['temperature'],
        d['humidity'],
        d['pressure'],
        d['weather_description'],
        d['wind_speed'],
        d['timestamp']
    ) for d in weather_data]
    
    # Batch insert
    execute_values(cur, """
        INSERT INTO weather_data (
            city, temperature, humidity, pressure, 
            weather_description, wind_speed, timestamp
        ) VALUES %s
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Fetch and store weather data from OpenWeatherMap API',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

fetch_weather = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather_data,
    dag=dag
)

store_weather = PythonOperator(
    task_id='store_weather',
    python_callable=store_weather_data,
    dag=dag
)

fetch_weather >> store_weather
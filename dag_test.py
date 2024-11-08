import os
import json
import requests
import psycopg2
from datetime import datetime, timedelta
from airflow.models import DagBag
from airflow.utils.db import get_session
from airflow.contrib.hooks.postgres_hook import PostgresHook
import pytest

@pytest.fixture
def dag_bag():
    return DagBag(dag_folder='dags', include_examples=False)

def test_dag_loaded(dag_bag):
    assert len(dag_bag.import_errors) == 0
    assert "weather_data_pipeline" in dag_bag.dags

def test_dag_structure(dag_bag):
    dag = dag_bag.get_dag('weather_data_pipeline')
    assert len(dag.tasks) == 2
    assert 'fetch_weather' in dag.task_ids
    assert 'store_weather' in dag.task_ids
    assert dag.tasks[0].task_id == 'fetch_weather'
    assert dag.tasks[1].task_id == 'store_weather'
    assert dag.tasks[0].downstream_task_ids == ['store_weather']

def test_fetch_weather_data(dag_bag, monkeypatch):
    def mock_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code
            
            def json(self):
                return self.json_data
            
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.exceptions.RequestException()

        with open('tests/sample_weather_data.json') as f:
            sample_data = json.load(f)

        return MockResponse(sample_data, 200)

    monkeypatch.setattr(requests, 'get', mock_requests_get)

    dag = dag_bag.get_dag('weather_data_pipeline')
    task = dag.get_task('fetch_weather')

    # Execute the task
    task.execute({})

    # Check that the weather data is stored in XCom
    weather_data = task.xcom_pull(key='weather_data')
    assert len(weather_data) == len(os.listdir('tests/sample_weather_data'))
    for data in weather_data:
        assert 'city' in data
        assert 'temperature' in data
        assert 'humidity' in data
        assert 'pressure' in data
        assert 'weather_description' in data
        assert 'wind_speed' in data
        assert 'timestamp' in data

def test_store_weather_data(dag_bag, monkeypatch):
    def mock_get_conn(*args, **kwargs):
        return psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="localhost"
        )

    monkeypatch.setattr(PostgresHook, 'get_conn', mock_get_conn)

    dag = dag_bag.get_dag('weather_data_pipeline')
    task = dag.get_task('store_weather')

    # Mock the weather data in XCom
    weather_data = [
        {
            'city': 'London',
            'temperature': 10.5,
            'humidity': 80,
            'pressure': 1020,
            'weather_description': 'Cloudy',
            'wind_speed': 5.2,
            'timestamp': datetime.now()
        },
        {
            'city': 'New York',
            'temperature': 15.0,
            'humidity': 70,
            'pressure': 1015,
            'weather_description': 'Sunny',
            'wind_speed': 3.8,
            'timestamp': datetime.now() - timedelta(hours=1)
        }
    ]
    task.xcom_push(key='weather_data', value=weather_data)

    # Execute the task
    task.execute({})

    # Check that the data is stored in the database
    with get_session() as session:
        stored_data = session.execute("SELECT * FROM weather_data").fetchall()
        assert len(stored_data) == 2
        for row in stored_data:
            assert row.city in ['London', 'New York']
            assert row.temperature in [10.5, 15.0]
            assert row.humidity in [80, 70]
            assert row.pressure in [1020, 1015]
            assert row.weather_description in ['Cloudy', 'Sunny']
            assert row.wind_speed in [5.2, 3.8]
            assert row.timestamp <= datetime.now()
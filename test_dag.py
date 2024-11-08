import pytest
from dags.weather_etl_dag_sample import fetch_weather_data, normalize_weather_data

def test_fetch_weather_data():
    context = {'task_instance': None}
    result = fetch_weather_data(**context)
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Test first city data structure
    first_city = result[0]
    assert 'id' in first_city
    assert 'name' in first_city
    assert 'weather' in first_city
    assert 'main' in first_city

def test_normalize_weather_data(mocker):
    # Mock the task instance
    mock_ti = mocker.Mock()
    mock_ti.xcom_pull.return_value = [{
        'id': 123,
        'name': 'Test City',
        'sys': {'country': 'TC'},
        'coord': {'lat': 0, 'lon': 0},
        'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}],
        'main': {
            'temp': 20,
            'feels_like': 19,
            'humidity': 50,
            'pressure': 1013
        },
        'wind': {'speed': 5}
    }]
    
    context = {'task_instance': mock_ti}
    
    # This should run without errors
    normalize_weather_data(**context)
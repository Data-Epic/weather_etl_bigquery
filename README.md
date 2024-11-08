#  Weather Data Pipeline
This Airflow DAG fetches weather data from the OpenWeatherMap API for specified cities and stores it in a PostgreSQL database. It runs on a schedule to ensure up-to-date weather information in the database.

## Overview
This pipeline automates fetching weather data for selected cities and storing it in a PostgreSQL table. It uses the OpenWeatherMap API to gather details such as temperature, humidity, pressure, and wind speed. Data is stored in a PostgreSQL table weather_data, which can be queried for analytical or reporting purposes.

### Prerequisites
Airflow installed and running.
PostgreSQL instance for data storage.
An OpenWeatherMap API key.

### DAG Structure
The DAG includes two main tasks:

1. Fetch Weather Data: Requests weather information from the OpenWeatherMap API for each specified city.
2. Store Weather Data: Saves the fetched data into a PostgreSQL database.
The DAG is configured to run every hour and includes error handling and retry logic for reliability.

### Setup

Using the Docker and Docker compose file we will create an apache airflow enviroment with the following code 

1. To Fetch  dockercompose file : curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.2/docker-compose.yaml'
2. Setting the right Airflow user on linux: mkdir -p ./dags ./logs ./plugins ./config, echo -e "AIRFLOW_UID=$(id -u)" > .env
3. Initialize the database: docker compose up airflow-init
4. Running Airflow: docker compose up

The Code Details

### fetch_weather_data
This function pulls weather data from OpenWeatherMap API for the list of cities specified in CITIES. Each city's data includes:

City name
Temperature
Humidity
Pressure
Weather description
Wind speed
Timestamp of data retrieval
### store_weather_data
This function connects to PostgreSQL, checks for an existing weather_data table, creates it if not present, and inserts the weather data batch-wise.

## Usage
Start the DAG in Airflow.
Monitor tasks in the Airflow UI to ensure data is fetched and stored correctly.
Query the PostgreSQL weather_data table for insights.



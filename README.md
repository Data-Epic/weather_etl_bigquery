# Weather Data ETL with Airflow and BigQuery

This project implements a normalized ETL pipeline using Apache Airflow to fetch weather data from OpenWeatherMap API and store it in Google BigQuery.

## Data Model


The data model follows 3NF normalization principles and consists of the following tables:

### cities

city_id (PK)
city_name
country_code
latitude
longitude


### weather_conditions

condition_id (PK)
condition_main
condition_description
icon_code


### measurements

measurement_id (PK)
city_id (FK, references cities.city_id)
condition_id (FK, references weather_conditions.condition_id)
temperature
feels_like
humidity
pressure
wind_speed
timestamp



The tables are partitioned by relevant dimensions to optimize query performance and reduce storage costs. For example, the measurements table is partitioned by the timestamp column, while the cities and weather_conditions tables are partitioned by the system-generated _PARTITIONTIME column.
Execution Plan
The ETL pipeline follows these steps:

Fetch Weather Data: The DAG fetches current weather data for a predefined list of cities using the OpenWeatherMap API.
Normalize Data: The raw weather data is normalized into the three tables (cities, weather_conditions, measurements) to reduce redundancy and improve data integrity.
Load to BigQuery: The normalized data is then loaded into the corresponding BigQuery tables using the BigQueryInsertJobOperator.
Handle Errors and Retries: The pipeline includes mechanisms to handle API rate limits, network failures, and other errors. Failed tasks are automatically retried up to a configured number of times.
Schedule Execution: The DAG is scheduled to run hourly, ensuring that the weather data is regularly updated in the data warehouse.

## Setup and Configuration
To set up and run the Weather Data ETL pipeline

1. Set up Google Cloud Project and BigQuery:

Create a new Google Cloud project or use an existing one.
Enable the BigQuery API for your project.
Ensure you have the necessary permissions to create datasets and tables in BigQuery.


2. Prepare the Airflow Environment:

Set up an Apache Airflow environment, either locally or in a cloud-based platform like Google Cloud Composer.
Install the required Python packages by creating a requirements.txt file and running pip install -r requirements.txt.
Configure the Airflow connection to your Google Cloud project by setting the google_cloud_default connection.


Configure the DAG:

Update the PROJECT_ID and DATASET_ID variables in the DAG file to match your Google Cloud project and the desired BigQuery dataset.
Provide your OpenWeatherMap API key by updating the API_KEY variable.
Review the CITIES list and modify it if needed to include the cities you want to fetch weather data for.


Deploy the DAG:

Copy the weather_dag.py file to your Airflow's dags directory.
Trigger the DAG manually or wait for the scheduled execution to begin.



DAG Walkthrough
The Weather Data ETL DAG consists of the following tasks:

Create Tables: The DAG first creates the necessary BigQuery tables (cities, weather_conditions, measurements) if they don't already exist.
Fetch Weather Data: This task fetches the current weather data for the configured list of cities using the OpenWeatherMap API.
Normalize Weather Data: The raw weather data is transformed and normalized into the three data tables.
Load to BigQuery: The normalized data is then loaded into the corresponding BigQuery tables using the BigQueryInsertJobOperator.

The tasks are set up with the following dependencies:

The table creation tasks (create_cities_table, create_conditions_table, create_measurements_table) run first.
The fetch_weather_data task runs after the table creation tasks.
The normalize_weather_data task runs after the fetch_weather_data task.

This workflow ensures that the necessary tables are created before attempting to load data into them.
Testing and Deployment
The project includes a GitHub Actions workflow that handles the following:







Deployment:

Deploys the DAG file to a Google Cloud Storage (GCS) bucket when changes are merged to the main branch.
Verifies the existence of the BigQuery dataset.



The workflow is triggered on both push and pull request events, ensuring that any changes to the DAG are thoroughly tested and linted before they are deployed to the production environment.
Monitoring and Maintenance
To ensure the ongoing health and reliability of the Weather Data ETL pipeline, consider the following monitoring and maintenance practices:

Airflow Monitoring:

Use Airflow's built-in monitoring features to track DAG runs, task status, and any errors or failures.
Set up email or Slack notifications for failed task instances or DAG runs.


BigQuery Monitoring:

Monitor the growth and partitioning of your BigQuery tables to ensure efficient storage and query performance.
Set up alerts for any unexpected changes in data volume or anomalies.


API Monitoring:

Monitor the OpenWeatherMap API for any changes in rate limits or service availability.
Adjust the DAG configuration and retries as needed to accommodate any changes.


Scheduled Maintenance:

Regularly review the data model and ETL logic to identify any necessary improvements or adjustments.
Perform schema migrations or table restructuring as needed to maintain data quality and performance.



Future Improvements
As you continue to develop and maintain the Weather Data ETL pipeline, consider the following potential improvements:

Incremental Loads: Implement an incremental loading strategy to only fetch and load new data, rather than fetching and processing the entire dataset on each run.
Data Quality Checks: Introduce data quality checks to ensure the integrity and accuracy of the fetched weather data before loading it into BigQuery.
Advanced Analytics: Explore building additional data products or dashboards on top of the Weather Data warehouse, such as weather trend analysis, anomaly detection, or predictive models.
Horizontal Scalability: If the data volume or processing requirements increase significantly, consider migrating to a more scalable data processing framework, such as Spark or Dataflow, to handle the increased load.
Automated Alerting and Notifications: Set up more advanced alerting and notification systems to proactively notify relevant stakeholders about pipeline issues, data anomalies, or other important events.

By continuously monitoring, optimizing, and enhancing the Weather Data ETL pipeline, you can ensure that your data warehouse remains a reliable and valuable asset for your organization.
FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    apt-get clean

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy Google Cloud credentials
COPY google_credentials.json /opt/airflow/google_credentials.json

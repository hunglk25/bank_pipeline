# Dockerfile
FROM apache/airflow:2.9.1-python3.10
COPY requirements.txt .
RUN pip install -r requirements.txt
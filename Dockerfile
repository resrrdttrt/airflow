# Base image from AIRFLOW_IMAGE_NAME
FROM apache/airflow:2.7.3

# Install additional libraries from requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

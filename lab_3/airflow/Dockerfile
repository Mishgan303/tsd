ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1
FROM ${AIRFLOW_BASE_IMAGE}

COPY requirements-classifier.txt /requirements-classifier.txt
RUN pip install --no-cache-dir -r /requirements-classifier.txt

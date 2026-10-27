FROM apache/airflow:3.3.2 AS base

USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt

FROM base AS test

USER airflow
COPY requirements-dev.txt /tmp/airflow-requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements-dev.txt

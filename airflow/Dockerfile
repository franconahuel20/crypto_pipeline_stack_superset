
FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl git && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir requests pandas sqlalchemy psycopg[binary]==3.2.* dbt-core==1.8.* dbt-postgres==1.8.*

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
WORKDIR /opt/airflow

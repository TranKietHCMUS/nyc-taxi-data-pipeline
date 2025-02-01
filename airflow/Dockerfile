FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y libgomp1

USER airflow

RUN pip install --upgrade pip

# This is to fix a bug in Airflow with PostgreSQL connection
RUN pip install git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f

RUN pip install --upgrade apache-airflow-providers-openlineage>=1.8.0

# Requirements
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
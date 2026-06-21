FROM apache/airflow:2.9.0
USER root
RUN apt-get update \
    && apt-get install -y default-jdk \
    && apt-get clean
USER airflow
RUN pip install \
    pyspark==3.5.0 \
    psycopg2-binary
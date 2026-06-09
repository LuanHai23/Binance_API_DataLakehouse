FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean

USER airflow
RUN pip install gcsfs

RUN cd /opt/bitnami/spark/jars && \
    curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

    COPY requirements.txt /tmp/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
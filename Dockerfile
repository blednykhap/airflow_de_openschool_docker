FROM apache/airflow:2.7.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless wget \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir apache-airflow-providers-apache-spark

RUN pip install --force-reinstall --no-cache-dir pyspark==3.2.0

RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir plyvel loguru fire

RUN cd /tmp
RUN mkdir -p jars
RUN wget "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar"
RUN mv postgresql-42.3.1.jar jars/

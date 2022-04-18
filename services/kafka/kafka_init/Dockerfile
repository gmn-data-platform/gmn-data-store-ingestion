FROM python:3.9-slim-bullseye

ARG KAFKA_BROKER_URL
ARG SCHEMA_REGISTRY_URL
ARG KSQLDB_SERVER_URL
ARG ELASTICSEARCH_URL
ARG ELASTICSEARCH_KAFKA_CONNECT_URL

ENV KAFKA_BROKER_URL=KAFKA_BROKER_URL
ENV SCHEMA_REGISTRY_URL=SCHEMA_REGISTRY_URL
ENV KSQLDB_SERVER_URL=KSQLDB_SERVER_URL
ENV ELASTICSEARCH_URL=ELASTICSEARCH_URL
ENV ELASTICSEARCH_KAFKA_CONNECT_URL=ELASTICSEARCH_KAFKA_CONNECT_URL

ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y librdkafka1 \
                       librdkafka-dev \
                       git

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

CMD python -u /kafka_init/main.py --kafka-broker-url $KAFKA_BROKER_URL --schema-registry-url $SCHEMA_REGISTRY_URL --ksqldb-server-url $KSQLDB_SERVER_URL --elasticsearch-url $ELASTICSEARCH_URL --elasticsearch-kafka-connect-url $ELASTICSEARCH_KAFKA_CONNECT_URL
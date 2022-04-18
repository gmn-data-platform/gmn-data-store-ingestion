FROM python:3.9-slim-bullseye

ARG KAFKA_BROKER_URL
ARG SCHEMA_REGISTRY_URL

ENV KAFKA_BROKER_URL=KAFKA_BROKER_URL
ENV SCHEMA_REGISTRY_URL=SCHEMA_REGISTRY_URL

ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y librdkafka1 \
                       librdkafka-dev \
                       git

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install --upgrade -r requirements.txt

CMD python -u  /kafka_database_sink/main.py --kafka-broker-url $KAFKA_BROKER_URL --schema-registry-url $SCHEMA_REGISTRY_URL
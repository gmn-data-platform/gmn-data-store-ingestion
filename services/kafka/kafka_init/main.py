"""
Script for setting up Kafka. Sets up topics, saves Avro schemas in the Kafka Schema
 Registry and sets up Elasticsearch Kafka connector.
"""
import json
import argparse
import sys
import time
import requests
from ksql import KSQLAPI

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from gmn_python_api.meteor_summary_schema import get_meteor_summary_avro_schema
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, \
    SchemaRegistryError
from requests.exceptions import ConnectionError

TRAJECTORY_SUMMARY_TOPIC_NAME = "trajectory_summary_raw"
TRAJECTORY_SUMMARY_SUBJECT_NAME = TRAJECTORY_SUMMARY_TOPIC_NAME + "-value"


def setup_kafka_topics(kafka_broker_url):
    while True:
        a = AdminClient({'bootstrap.servers': kafka_broker_url})
        topics = a.list_topics()  # Test kafka broker response
        if topics:
            break
        print('Waiting for kafka broker to be available...')
        time.sleep(1)

    main_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in
                   [TRAJECTORY_SUMMARY_TOPIC_NAME]]

    # Call create_topics to asynchronously create topics. A dict
    # of <topic,future> is returned.
    fs = a.create_topics(main_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def setup_schema_registry(schema_registry_url):
    schema = get_meteor_summary_avro_schema()
    avro_schema = Schema(json.dumps(schema), 'AVRO')

    schema_registry_client = SchemaRegistryClient({
        'url': "http://" + schema_registry_url
    })

    while True:
        try:
            latest_schema_version = schema_registry_client.get_latest_version(
                TRAJECTORY_SUMMARY_SUBJECT_NAME)

            if avro_schema.schema_str.replace(" ", "") == \
                    latest_schema_version.schema.schema_str.replace(" ", ""):
                print('Schema is already registered.')
                return
            else:
                print("Schema is not the latest version. Adding new version.")
            break
        except ConnectionError as e:
            print(e)
            print('Waiting for schema registry to be available...')
            time.sleep(1)
        except SchemaRegistryError as e:
            print("Existing schema not found. Adding a schema.")
            break

    _schema_id = schema_registry_client.register_schema(TRAJECTORY_SUMMARY_SUBJECT_NAME,
                                                        avro_schema)
    print('Registered new schema with id: {}'.format(_schema_id))
    # print('Schema: {}'.format(avro_schema.schema_str))


def setup_kafka_with_ksql(ksqldb_server_url, schema_registry_url, elasticsearch_url,
                          elasticsearch_kafka_connect_url):
    while True:
        try:
            client = KSQLAPI("http://" + ksqldb_server_url)
            break
        except Exception as e:
            print(e)
            print('Waiting for ksqldb server to be available...')
            time.sleep(1)

    while True:
        try:
            requests.get("http://" + elasticsearch_kafka_connect_url)
            break
        except Exception as e:
            print(e)
            print('Waiting for elasticsearch kafka connect to be available...')
            time.sleep(1)

    client.ksql("CREATE STREAM IF NOT EXISTS trajectory_summary_raw_stream "
                "WITH (KAFKA_TOPIC='trajectory_summary_raw', VALUE_FORMAT='AVRO')")

    client.ksql(f"""CREATE SINK CONNECTOR SINK_ELASTIC_1 WITH (
      'connector.class'                     = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
      'connection.url'                      = '{"http://" + elasticsearch_url}',
      'value.converter'                     = 'io.confluent.connect.avro.AvroConverter',
      'value.converter.schema.registry.url' = '{"http://" + schema_registry_url}',
      'tasks.max'                           = '1',
      'type.name'                           = '_doc',
      'topics'                              = '{TRAJECTORY_SUMMARY_TOPIC_NAME}',
      'key.ignore'                          = 'true',
      'schema.ignore'                       = 'true',
      'schema.enable'                       = 'false',
      'behavior.on.null.values'             = 'IGNORE',
      'behavior.on.malformed.documents'     = 'IGNORE'
    )""")


if __name__ == '__main__':
    print(sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka-broker-url', required=True)
    parser.add_argument('--schema-registry-url', required=True)
    parser.add_argument('--ksqldb-server-url', required=True)
    parser.add_argument('--elasticsearch-url', required=True)
    parser.add_argument('--elasticsearch-kafka-connect-url', required=True)
    args = parser.parse_args()

    setup_kafka_topics(args.kafka_broker_url)
    setup_schema_registry(args.schema_registry_url)
    setup_kafka_with_ksql(args.ksqldb_server_url, args.schema_registry_url,
                          args.elasticsearch_url, args.elasticsearch_kafka_connect_url)

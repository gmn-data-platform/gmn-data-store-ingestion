"""Script to consume Kafka trajectory summary messages into the GMN Data Store."""
import argparse
import math
import sys

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from gmn_data_store.controller import insert_trajectory_summary

TRAJECTORY_SUMMARY_TOPIC_NAME = "trajectory_summary_raw"

if __name__ == '__main__':
    print(sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka-broker-url', type=str, required=True)
    parser.add_argument('--schema-registry-url', type=str, required=True)
    args = parser.parse_args()

    c = AvroConsumer({
        'bootstrap.servers': args.kafka_broker_url,
        'group.id': 'kafkadbsinkgroup',
        'enable.auto.commit': False,
        # offset will be committed manually after database upsert
        'schema.registry.url': 'http://' + args.schema_registry_url})

    c.subscribe([TRAJECTORY_SUMMARY_TOPIC_NAME])

    print(
        "Start consuming Kafka messages from last committed offset in group"
        " kafkadbsinkgroup")
    while True:
        try:
            msg = c.poll(10)  # timeout 10 seconds
            print(f"Polled {TRAJECTORY_SUMMARY_TOPIC_NAME}")

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            break

        if msg is None:
            print("No message received")
            continue

        if msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            continue

        msg_value = msg.value()

        # Convert any nan float values to None
        for key in msg_value.keys():
            if isinstance(msg_value[key], float) and math.isnan(msg_value[key]):
                msg_value[key] = None

        print("Upserting message: ", msg_value)
        try:
            insert_trajectory_summary(msg_value)
        except Exception as e:
            print(f"Error inserting trajectory summary: {e}, message: {msg_value}")
            continue
        print("Successfully upserted message into the GMN Data store")

        c.commit(msg)  # commit offset
    c.close()

import sys
import os
import logging

from confluent_kafka import Consumer, KafkaException, KafkaError
from google.cloud import pubsub_v1

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

PROJECT_ID = os.environ.get('PROJECT_ID')
TOPIC_ID = os.environ.get('TOPIC_ID')


# Resolve the publish future in a separate thread.
def callback(future) -> None:
    message_id = future.result()
    logging.info(f"Message ID: {message_id}")


batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=2048000,  # One kilobyte
    max_latency=5,  # One second
)

publisher_options = pubsub_v1.types.PublisherOptions(
    flow_control=pubsub_v1.types.PublishFlowControl(
        message_limit=500,
        byte_limit=2 * 1024 * 1024,
        limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
    ),
)

publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings, publisher_options=publisher_options)
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT_ID,
    topic=TOPIC_ID,
)

topics = os.environ['CLOUDKARAFKA_TOPIC'].split(",")

# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
    'group.id': "%s-consumer" % os.environ['CLOUDKARAFKA_USERNAME'],
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'earliest'},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
    'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
}

c = Consumer(**conf)
c.subscribe(topics)

while True:
    msg = c.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        # Error or event
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            # Error
            raise KafkaException(msg.error())
    else:
        # Proper message
        print(msg.value())
        publish_future = publisher.publish(topic_name, msg.value())
        publish_future.add_done_callback(callback)

# Close down consumer to commit final offsets.
c.close()

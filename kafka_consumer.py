import datetime
import functools
import json
import os
import logging
import time

from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from google.cloud import pubsub_v1
from concurrent import futures

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

PROJECT_ID = os.environ.get('PROJECT_ID')
TOPIC_ID = os.environ.get('TOPIC_ID')


# Resolve the published future in a separate thread.
def callback(future, metadata) -> None:
    message_id = future.result()
    # logging.info(f"Message ID: {message_id}")


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
    # enable.auto.offset.store: false means turn off auto update on the local offset store but manage it manually
    'default.topic.config': {'auto.offset.reset': 'earliest', 'enable.auto.offset.store': False},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
    'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
}

c = Consumer(**conf)
c.subscribe(topics)

BATCH_SIZE=500
publish_futures = []
publish_futures_offset_tracker = {}
start_time = time.time()
seconds = 4

while True:
    current_time = time.time()
    elapsed_time = current_time - start_time

    msg = c.poll(timeout=1.0)
    """ 
    Manage offset on each partition and topic independently by making sure only the latest + 1
    offset is committed when all requests are sent to Pub/Sub successfully. 
    
    If any of the sends have failed, the pubsub client will automatically retry for a period of time, 
    if they are still failed a exception will be raised and the whole process terminates.
    """
    if msg is None and publish_futures or len(publish_futures) > 0 and len(publish_futures) % BATCH_SIZE == 0:
        logging.info("Iteration since the last one: " + str(int(elapsed_time)) + " seconds")
        done, not_done = futures.wait(fs=publish_futures, return_when=futures.FIRST_EXCEPTION)

        for f in done:
            if f.exception():
                raise RuntimeError("terminating consumer due to exceptions...")

        topic_partitions = []
        for topic, partitions in publish_futures_offset_tracker.items():
            for partition, offsets in partitions.items():
                offset_to_commit = max(offsets) + 1
                logging.info(f"Committing offset for topic partition:  {topic}-{partition}--{offset_to_commit}")
                topic_partitions.append(TopicPartition(topic, partition, offset_to_commit))

        c.store_offsets(offsets=topic_partitions)

        # all futures are done, reinitialise..
        start_time = time.time()
        publish_futures = []
        publish_futures_offset_tracker = {}
        continue
    elif msg is None:
        continue

    if msg.error():
        # Error or event
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            logging.error('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            # Error
            raise KafkaException(msg.error())
    else:
        # Add additional metadata fields
        msg_dict = json.loads(msg.value())
        msg_dict['metadata'] = {}
        msg_dict['metadata']['kafka_topic'] = msg.topic()
        msg_dict['metadata']['kafka_partition'] = msg.partition()
        msg_dict['metadata']['kafka_offset'] = msg.offset()
        msg_dict['metadata']['kafka_consumer_timestamp'] = datetime.datetime.now().isoformat()
        # print(msg_dict)
        msg_to_send = json.dumps(msg_dict).encode('utf-8')

        publish_future = publisher.publish(topic_name, msg_to_send)
        publish_future.add_done_callback(functools.partial(callback, metadata={
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset()
        }))
        logging.info(f"Message metadata: {msg.topic()},{msg.partition()},{msg.offset()}")
        publish_futures.append(publish_future)
        if msg.topic() not in publish_futures_offset_tracker:
            publish_futures_offset_tracker[msg.topic()] = {}

        if msg.partition() not in publish_futures_offset_tracker[msg.topic()]:
            publish_futures_offset_tracker[msg.topic()][msg.partition()] = []
            publish_futures_offset_tracker[msg.topic()][msg.partition()].append(msg.offset())
        else:
            publish_futures_offset_tracker[msg.topic()][msg.partition()].append(msg.offset())

        logging.info(f"Offsite tracker: {json.dumps(publish_futures_offset_tracker)}")

# Close down consumer to commit final offsets.
c.close()

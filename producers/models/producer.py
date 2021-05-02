"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

KAFKA_URL = 'PLAINTEXT://localhost:9092'
SCHEMA_URL = 'http://localhost:8081'

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
           'bootstrap.servers': KAFKA_URL,
            'schema.registry.url': SCHEMA_URL
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient({"bootstrap.servers": 'PLAINTEXT://localhost:9092'})
        topic_metadata = client.list_topics(timeout=5)
        exists = self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))
        if exists is False:
            futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas
                    )
                ]
            )
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info("topic creation kafka integration complete!")
                except Exception as e:
                    logger.info(f"failed to create topic {self.topic_name}: {e}")
        else:
            logger.info("topic already exists - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        self.producer.flush()
        logger.info(f"{self.topic_name} producer flushed!")


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

'''
Producer base-class providing common utilites and functionality
'''

import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

import constants
import utils

logger = utils.get_logger(__name__)


class Producer:

    '''
    Defines and provides common functionality amongst Producers
    '''

    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):

        '''
        Initializes a Producer object with basic settings
        '''

        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties
        self.broker_properties = {
            'bootstrap.servers': constants.KAFKA,
            'schema.registry.url': constants.SCHEMA_REGISTRY
        }

        # Create the topic
        self.create_topic()

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):

        '''
        Creates the producer topic if it does not already exist
        '''

        if self.topic_name not in Producer.existing_topics:
            logger.info('creating topic {}'.format(self.topic_name))
            topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas
            )
            client = AdminClient({
                'bootstrap.servers': constants.KAFKA
            })
            client.create_topics([topic])
            Producer.existing_topics.add(self.topic_name)

    def close(self):

        '''
        Prepares the producer for exit by cleaning up the producer
        '''

        if self.producer is not None:
            logger.info('flushing the producer')
            self.producer.flush()

    @classmethod
    def time_millis(cls):

        return int(round(time.time() * 1000))

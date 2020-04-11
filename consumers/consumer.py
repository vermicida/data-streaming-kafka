'''
Defines core consumer functionality
'''

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

import constants
import utils

logger = utils.get_logger(__name__)


class KafkaConsumer:

    '''
    Defines the base kafka consumer class
    '''

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):

        '''
        Creates a consumer object for asynchronous use
        '''

        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            'bootstrap.servers': constants.KAFKA,
            'group.id': topic_name_pattern,
            'default.topic.config': {'auto.offset.reset': 'earliest'}
        }

        if is_avro is True:
            self.broker_properties['schema.registry.url'] = constants.SCHEMA_REGISTRY
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):

        '''
        Callback for when topic assignment takes place
        '''

        for partition in partitions:
            if self.offset_earliest is True:
                partition.offset = OFFSET_BEGINNING

        logger.info(f'{self.topic_name_pattern} ok!')
        consumer.assign(partitions)

    async def consume(self):

        '''
        Asynchronously consumes data from kafka topic
        '''

        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):

        '''
        Polls for a message. Returns 1 if a message was received, 0 otherwise
        '''

        try:
            message = self.consumer.poll(1.0)
        except SerializerError as er:
            logger.error(f'serialization error: {er.message}')
            result = 0
        else:
            if message is None:
                logger.debug('message is none')
                result = 0
            else:
                error = message.error()
                if error is None:
                    self.message_handler(message)
                    result = 1
                else:
                    logger.debug(f'message error: {error}')
                    result = 0
        return result

    def close(self):

        '''
        Cleans up any open kafka consumers
        '''

        if self.consumer is not None:
            logger.info('closing the consumer')
            self.consumer.close()

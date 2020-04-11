'''
Creates a turnstile data producer
'''

import constants
import utils
from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = utils.get_logger(__name__)


class Turnstile(Producer):

    def __init__(self, station):

        '''
        Create the Turnstile
        '''

        key_schema = utils.load_avro_schema('turnstile_key.json')
        value_schema = utils.load_avro_schema('turnstile_value.json')

        super().__init__(
            constants.TOPIC_TURNSTILE_V1,
            key_schema,
            value_schema=value_schema,
            num_partitions=5
        )

        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):

        '''
        Simulates riders entering through the turnstile
        '''

        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        logger.info(f'{num_entries} riders entered the station {self.station.station_id}')

        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={'timestamp': self.time_millis()},
                value={
                    'station_id': self.station.station_id,
                    'station_name': self.station.name,
                    'line': self.station.color.name
                }
            )

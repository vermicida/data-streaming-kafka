'''
Methods pertaining to weather data
'''

import json
import random
from enum import IntEnum

import utils
import constants
from models.producer import Producer

logger = utils.get_logger(__name__)


class Weather(Producer):

    '''
    Defines a simulated weather model
    '''

    status = IntEnum(
        'status',
        'sunny partly_cloudy cloudy windy precipitation',
        start=0
    )

    key_schema = None
    value_schema = None

    winter_months = (0, 1, 2, 3, 10, 11)
    summer_months = (6, 7, 8)

    def __init__(self, month):

        super().__init__(
            constants.TOPIC_WEATHER_V1,
            Weather.key_schema,
            value_schema=Weather.value_schema
        )

        self.status = Weather.status.sunny

        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0
        else:
            self.temp = 70.0

        Weather.key_schema = utils.load_json_schema('weather_key.json')
        Weather.value_schema = utils.load_json_schema('weather_value.json')

    def _set_weather(self, month):

        '''
        Returns the current weather
        '''

        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        else:
            mode = 0.0

        self.temp += min(
            max(-20.0, random.triangular(-10.0, 10.0, mode)),
            100.0
        )
        self.status = random.choice(list(Weather.status))

    def run(self, month):

        self._set_weather(month)

        response = utils.do_request(
            f'{constants.REST_PROXY}/topics/{self.topic_name}',
            method='POST',
            headers={'Content-Type': 'application/vnd.kafka.avro.v2+json'},
            data={
                'key_schema': json.dumps(Weather.key_schema),
                'value_schema': json.dumps(Weather.value_schema),
                'records': [{
                    'value': {
                        'temperature': int(self.temp),
                        'status': self.status.name
                    },
                    'key': {
                        'timestamp': self.time_millis()
                    }
                }]
            }
        )
        response.raise_for_status()

        logger.debug(f'weather is {self.temp}ºF {self.status.name}')

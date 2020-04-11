'''
Contains functionality related to Weather
'''

import utils

logger = utils.get_logger(__name__)


class Weather:

    '''
    Defines the Weather model
    '''

    def __init__(self):

        '''
        Creates the weather model
        '''

        self.temperature = 70.0
        self.status = 'sunny'

    def process_message(self, message):

        '''
        Handles incoming weather data
        '''

        if 'weather' in message.topic():
            weather = message.value()
            if weather is not None:
                if 'temperature' in weather:
                    self.temperature = weather['temperature']
                if 'status' in weather:
                    self.status = weather['status']

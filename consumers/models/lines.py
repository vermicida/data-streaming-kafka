'''
Contains functionality related to Lines
'''

import json

import constants
import utils
from models import Line

logger = utils.get_logger(__name__)


class Lines:

    '''
    Contains all train lines
    '''

    def __init__(self):

        '''
        Creates the Lines object
        '''

        self.red_line = Line('red')
        self.green_line = Line('green')
        self.blue_line = Line('blue')

    def process_message(self, message):

        '''
        Processes a station message
        '''

        topic = message.topic()

        if constants.TOPIC_STATION in topic:
            value = message.value()
            if topic == constants.TOPIC_STATIONS_TABLE_V1:
                value = json.loads(value)
            if value['line'] == 'green':
                self.green_line.process_message(message)
            elif value['line'] == 'red':
                self.red_line.process_message(message)
            elif value['line'] == 'blue':
                self.blue_line.process_message(message)
            else:
                logger.debug('discarding unknown line msg %s', value['line'])

        elif constants.TOPIC_TURNSTILE_SUMMARY == topic:
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)

        else:
            logger.info('ignoring non-lines message %s', topic)

'''
Defines a Tornado Server that consumes Kafka Event data for display
'''

import tornado.ioloop
import tornado.template
import tornado.web

import constants
import utils
from consumer import KafkaConsumer
from models import Lines, Weather

logger = utils.get_logger(__name__)


class MainHandler(tornado.web.RequestHandler):

    '''
    Defines a web request handler class
    '''

    template_dir = tornado.template.Loader(utils.get_templates_path())
    template = template_dir.load('status.html')

    def initialize(self, weather, lines):

        '''
        Initializes the handler with required configuration
        '''

        self.weather = weather
        self.lines = lines

    def get(self):

        '''
        Responds to get requests
        '''

        logger.debug('rendering and writing handler template')
        self.write(MainHandler.template.generate(
            weather=self.weather,
            lines=self.lines
        ))


def run_server():

    '''
    Runs the Tornado Server and begins Kafka consumption
    '''

    if not utils.topic_exists(constants.TOPIC_TURNSTILE_SUMMARY):
        logger.fatal('Ensure that the KSQL Command has run successfully before running the web server!')
        exit(1)
    if not utils.topic_exists(constants.TOPIC_STATIONS_TABLE_V1):
        logger.fatal('Ensure that Faust Streaming is running successfully before running the web server!')
        exit(1)

    weather_model = Weather()
    lines = Lines()

    application = tornado.web.Application(
        [(r'/', MainHandler, {'weather': weather_model, 'lines': lines})]
    )
    application.listen(constants.SERVER_PORT)

    # Build kafka consumers
    consumers = [
        KafkaConsumer(
            constants.TOPIC_WEATHER_V1,
            weather_model.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            constants.TOPIC_STATIONS_TABLE_V1,
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
        KafkaConsumer(
            constants.TOPIC_ARRIVALS_REGEX,
            lines.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            constants.TOPIC_TURNSTILE_SUMMARY,
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        )
    ]

    try:
        logger.info(f'Transit Status Page running at http://localhost:{constants.SERVER_PORT}')
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        logger.info('shutting down server')
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()


if __name__ == '__main__':
    run_server()

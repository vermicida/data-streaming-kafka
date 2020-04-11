'''
Configures a Kafka Connector for Postgres Station data
'''

import constants
import utils

logger = utils.get_logger(__name__)

CONNECTOR_NAME = 'stations'


def _connector_exists():

    '''
    Checks if the connector exists
    '''

    response = utils.do_request(f'{constants.KAFKA_CONNECT}/connectors/{CONNECTOR_NAME}')
    return response.status_code == 200


def _create_connector():

    '''
    Creates a new connector
    '''

    response = utils.do_request(
        f'{constants.KAFKA_CONNECT}/connectors',
        method='POST',
        data={
            'name': CONNECTOR_NAME,
            'config': {
                'connector.class': 'io.confluent.connect.jdbc.JdbcSourceConnector',
                'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
                'key.converter.schemas.enable': 'false',
                'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
                'value.converter.schemas.enable': 'false',
                'batch.max.rows': 500,
                'connection.url': constants.POSTGRESQL_CONN,
                'connection.user': constants.POSTGRESQL_USER,
                'connection.password': constants.POSTGRESQL_PASS,
                'table.whitelist': 'stations',
                'mode': 'incrementing',
                'incrementing.column.name': 'stop_id',
                'topic.prefix': constants.TOPIC_PREFIX,
                'poll.interval.ms': 60 * 1000  # every minute
            }
        }
    )
    response.raise_for_status()


def configure_connector():

    '''
    Starts and configures the Kafka Connect connector
    '''

    logger.debug('creating or updating kafka connect connector...')
    if _connector_exists():
        logger.debug('connector already created skipping recreation')
    else:
        _create_connector()
        logger.debug('connector created successfully')


if __name__ == '__main__':
    configure_connector()

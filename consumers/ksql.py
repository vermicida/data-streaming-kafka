'''
Configures KSQL to combine station and turnstile data
'''

import constants
import utils

logger = utils.get_logger(__name__)

KSQL_STATEMENT = f'''
CREATE TABLE turnstile (station_id INT,
                        station_name VARCHAR,
                        line VARCHAR)
        WITH (kafka_topic = '{constants.TOPIC_TURNSTILE_V1}',
              value_format = 'avro',
              key = 'station_id');

CREATE TABLE turnstile_summary
        WITH (value_format = 'json') AS
      SELECT station_id,
             COUNT(station_id) AS count
        FROM turnstile
    GROUP BY station_id;
'''


def execute_statement():

    '''
    Executes the KSQL statement against the KSQL API
    '''

    if utils.topic_exists(constants.TOPIC_TURNSTILE_SUMMARY):
        return

    logger.debug('executing ksql statement...')

    response = utils.do_request(
        f'{constants.KSQL}/ksql',
        method='POST',
        headers={'Content-Type': 'application/vnd.ksql.v1+json'},
        data={
            'ksql': KSQL_STATEMENT,
            'streamsProperties': {'ksql.streams.auto.offset.reset': 'earliest'}
        }
    )
    response.raise_for_status()


if __name__ == '__main__':
    execute_statement()

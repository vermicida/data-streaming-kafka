'''
Defines trends calculations for stations
'''

import faust

import constants
import utils

logger = utils.get_logger(__name__)


class Station(faust.Record):

    '''
    Faust will ingest records from Kafka in this format
    '''

    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


class TransformedStation(faust.Record):

    '''
    Faust will produce records to Kafka in this format
    '''

    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App(
    'stations-stream',
    broker=constants.KAFKA_BROKER,
    store='memory://'
)

topic = app.topic(
    constants.TOPIC_STATIONS,
    value_type=Station
)

out_topic = app.topic(
    constants.TOPIC_STATIONS_TABLE_V1,
    partitions=1
)

table = app.Table(
   constants.TOPIC_STATIONS_TABLE_V1,
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def process(stream):
    async for event in stream:
        if event.red is True:
            line = 'red'
        elif event.blue is True:
            line = "blue"
        elif event.green is True:
            line = "green"
        else:
            line = None

        if line is None:
            logger.warning("couldnt find line color for %s", event)
            continue

        table[event.station_id] = TransformedStation(
            station_id=event.station_id,
            station_name=event.station_name,
            order=event.order,
            line=line,
        )


if __name__ == "__main__":
    app.main()

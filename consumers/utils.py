import json
import logging
import logging.config
import os
from confluent_kafka.admin import AdminClient
from requests import Request, Session

import constants

config_path = os.path.join(os.getcwd(), 'logging.ini')
logging.config.fileConfig(config_path)

session = None


def get_logger(name):
    return logging.getLogger(name)


def get_session():
    global session
    if session is None:
        session = Session()
    return session


def do_request(url, method='GET', **kwargs):
    kwargs['url'] = url
    kwargs['method'] = method
    if 'headers' not in kwargs:
        kwargs['headers'] = {'Content-Type': 'application/json'}
    if 'data' in kwargs:
        data = json.dumps(kwargs['data']) \
            if isinstance(kwargs['data'], dict) \
            else str(kwargs['data'])
        kwargs['data'] = data
    request = Request(**kwargs)
    return get_session().send(request.prepare())


def get_templates_path(name=None):
    return os.path.join(os.getcwd(), 'templates') \
        if name is None \
        else os.path.join(os.getcwd(), 'templates', name)


def topic_exists(topic):
    client = AdminClient({'bootstrap.servers': constants.KAFKA})
    topic_metadata = client.list_topics(timeout=5)
    print(set(t.topic for t in iter(topic_metadata.topics.values())))
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))

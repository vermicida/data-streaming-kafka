import json
import logging
import logging.config
import os
import pandas as pd
from confluent_kafka import avro
from requests import Request, Session

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


def get_schema_path(name=None):
    return os.path.join(os.getcwd(), 'models', 'schemas') \
        if name is None \
        else os.path.join(os.getcwd(), 'models', 'schemas', name)


def get_data_path(name=None):
    return os.path.join(os.getcwd(), 'data') \
        if name is None \
        else os.path.join(os.getcwd(), 'data', name)


def load_avro_schema(name):
    path = get_schema_path(name)
    return avro.load(path)


def load_json_schema(name):
    path = get_schema_path(name)
    with open(path) as f:
        schema = json.load(f)
    return schema


def load_csv_dataframe(name):
    path = get_data_path(name)
    return pd.read_csv(path)

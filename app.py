from datetime import datetime, UTC
from flask import Flask, jsonify, request, make_response
from typing import cast
import logging
import os
import random
import uuid
import psycopg2
import requests
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
from pymongo import MongoClient


# OpenTelemetry Instrumentation
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.trace import get_tracer_provider
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
ElasticsearchInstrumentor().instrument()
PymongoInstrumentor().instrument()
inst = ConfluentKafkaInstrumentor()
tracer_provider = get_tracer_provider()
RequestsInstrumentor().instrument()
# End of OpenTelemetry Instrumentation

# System Performance
from opentelemetry.metrics import set_meter_provider
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
#
# exporter = ConsoleMetricExporter()
#
# set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))
# SystemMetricsInstrumentor().instrument()
#
# # metrics are collected asynchronously
# input("...")
#
# # to configure custom metrics
# configuration = {
#     "system.memory.usage": ["used", "free", "cached"],
#     "system.cpu.time": ["idle", "user", "system", "irq"],
#     "system.network.io": ["transmit", "receive"],
#     "process.memory.usage": None,
#     "process.memory.virtual": None,
#     "process.cpu.time": ["user", "system"],
#     "process.context_switches": ["involuntary", "voluntary"],
# }
# end of System Performance

log_level = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logging.getLogger('werkzeug').setLevel(logging.ERROR)
app = Flask(__name__)


def get_env_variable(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        else:
            raise ValueError(f"Environment variable '{var_name}' not set.")
    return value


def connect_to_database():
    db_host = get_env_variable("POSTGRES_HOST")
    db_port = get_env_variable("POSTGRES_PORT")
    db_name = get_env_variable("POSTGRES_DB")
    db_user = get_env_variable("POSTGRES_USER")
    db_password = get_env_variable("POSTGRES_PASSWORD")

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        logging.info("Successfully connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None

def publish_to_mongodb(records, message, transaction_id):
    mongodb_user = get_env_variable("MONGODB_USER")
    mongodb_password = get_env_variable("MONGODB_PASSWORD")
    mongo_host = get_env_variable("MONGODB_HOST")
    try:
        client = MongoClient(f"mongodb://{mongodb_user}:{mongodb_password}@{mongo_host}")
        db = client["local"]
        collection = db["weather"]
        document = {
            "id": transaction_id,
            "records": records,
            "content": message,
            "timestamp": datetime.now(UTC).isoformat()
        }
        collection.insert_one(document)
        logging.info("Published to MongoDB")
    except Exception as ex:
        logging.error(f"Error publishing to MongoDB: {ex}")


def publish_to_elastic(records, message, transaction_id):
    elastic_user = get_env_variable("ELASTIC_USER")
    elastic_password = get_env_variable("ELASTIC_PASSWORD")
    elastic_server = get_env_variable("ELASTIC_SERVER")
    try:
        es = Elasticsearch(elastic_server,
            basic_auth=(elastic_user, elastic_password),
            verify_certs=False  # for self-signed certs only
        )
        document = {
            "id": transaction_id,
            "records": records,
            "content": message,
            "timestamp": datetime.now(UTC).isoformat()
        }
        es.index(index="weather-inquires", id=document["id"], document=document)
        logging.info("Published to Elasticsearch")
    except Exception as ex:
        logging.error(f"Error publishing to Elasticsearch: {ex}")


def publish_to_kafka(records, message, transaction_id):
    kafka_server = get_env_variable("KAFKA_SERVER")
    conf = {
        'bootstrap.servers': kafka_server
    }
    producer = Producer(conf)
    producer = inst.instrument_producer(producer, tracer_provider)
    topic = "test"

    def delivery_report(err, _):
        if err is not None:
            logging.error(f"Error publishing to Kafka: {err}")
        else:
            logging.info("Published to Kafka")

    message = {
        "id": transaction_id,
        "records": records,
        "content": message
    }
    producer.produce(topic, value=str(message), callback=delivery_report)
    logging.info("Published to Kafka")
    producer.poll(0)
    logging.info("Flushing Kafka producer...")
    producer.flush()
    logging.info("Kafka producer flushed.")


def get_latest_weather(n: int, transaction_id: str):
    cur = None
    conn = None
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        cur.execute("SELECT * FROM get_latest_weather(%s)", (n,))
        results = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        records = len(results)
        message = "Weather Data Pulled From Database"
        publish_to_kafka(records, message, transaction_id)
        publish_to_elastic(records, message, transaction_id)
        publish_to_mongodb(records, message, transaction_id)
        response = [dict(zip(column_names, row)) for row in results]
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    return response


def request_log(component: str, payload:dict = None ):
    transaction_id = str(uuid.uuid4())
    request_message = {
        'message': 'Request',
        'component': component,
        'transactionId': transaction_id
    }
    if payload:
        request_message['payload'] = payload
    logging.info(request_message)
    return transaction_id


def response_log(transaction_id:str, component: str, return_code, payload:dict = None):
    response_message = {
        'message': 'Response',
        'component': component,
        'transactionId': transaction_id,
        'returnCode': return_code
    }
    if payload:
        response_message['payload'] = payload
    logging.info(response_message)


@app.route("/latest-weather")
def latest_weather():
    return_code = 200
    payload = {}
    n = request.args.get('n', default=None, type=cast(callable, int))
    component = 'latest-weather'
    transaction_id = request_log(component, {'n': n})
    if not n:
        try:
            # Simulating a call to another service
            url = get_env_variable("RANDOM_SERVICE_URL")
            response = requests.get(url)
            if response.status_code == 200:
                n = response.json()
                payload = get_latest_weather(n, transaction_id)
            else:
                return_code = response.status_code
                payload = {"error": "Error from random service", "returnCode": response.status_code}
        except Exception as ex:
            return_code = 500
            payload = {"error": "Internal Server Error", "details": str(ex)}
    response_log(transaction_id, component, return_code, payload)
    return make_response(jsonify(payload), return_code)


@app.route("/random")
def random_number():
    return_code = 200
    component = 'random'
    transaction_id = request_log(component)
    n = random.randint(-50, 1050)
    response_log(transaction_id, component, return_code, {'n': n})
    return jsonify(n)

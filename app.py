from datetime import datetime, UTC
from flask import Flask, jsonify, request, make_response
from faker import Faker
import logging
import json
import os
import redis
import uuid
import psycopg2
import requests
import random
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
from pymongo import MongoClient


# OpenTelemetry Instrumentation
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.trace import get_tracer_provider
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
ElasticsearchInstrumentor().instrument()
PymongoInstrumentor().instrument()
inst = ConfluentKafkaInstrumentor()
tracer_provider = get_tracer_provider()
RequestsInstrumentor().instrument()
RedisInstrumentor().instrument()
# End of OpenTelemetry Instrumentation

# System Performance
from opentelemetry.metrics import set_meter_provider
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

exporter = ConsoleMetricExporter()

set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))
SystemMetricsInstrumentor().instrument()
configuration = {
    "system.memory.usage": ["used", "free", "cached"],
    "system.cpu.time": ["idle", "user", "system", "irq"],
    "system.network.io": ["transmit", "receive"],
    "process.memory.usage": None,
    "process.memory.virtual": None,
    "process.cpu.time": ["user", "system"],
    "process.context_switches": ["involuntary", "voluntary"],
}
# end of System Performance

log_level = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logging.getLogger('werkzeug').setLevel(getattr(logging, log_level, logging.INFO))
app = Flask(__name__)
redis_client = None
INTERNAL_ERROR = "INTERNAL SERVER ERROR"


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

def member_search(user_id: str):
    user = None
    conn = None
    cursor = None
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, first_name, last_name 
            FROM members 
            WHERE user_id = %s
        """, (user_id,))

        result = cursor.fetchone()
        if result:
            user = {
                'userId': user_id,
                'firstName': result[1],
                'lastName': result[2]
            }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return user

def member_create(user_id: str):
    conn = None
    cursor = None
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        # For demo purposes we will just make up a member
        fake = Faker()
        first_name = fake.first_name()
        last_name = fake.last_name()
        user = {
            'userId': user_id,
            'firstName': first_name,
            'lastName': last_name
        }

        cursor.execute("INSERT INTO members (user_id, first_name, last_name) VALUES (%s, %s, %s);",
                       (user['userId'], user['firstName'], user['lastName']))
        conn.commit()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return user


def member_service(user_id):
    user = member_search(user_id)
    if not user:
        user = member_create(user_id)
    return user


def load_redis(user_data: dict) -> None:
    global redis_client
    if not redis_client:
        redis_host = get_env_variable("REDIS_HOST")
        redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    redis_client.set(user_data['userId'], json.dumps(user_data))


def check_redis(user_id: str) -> dict:
    global redis_client
    if not redis_client:
        redis_host = get_env_variable("REDIS_HOST")
        redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    return redis_client.get(user_id)

def get_member(user_id: str) -> dict:
    user_data = None
    payload = {
        'userId': user_id,
    }
    url = get_env_variable("MEMBER_MANAGEMENT_URL")
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        user_data = response.json()
    return user_data


def authenticate_user(user_id: str):
    # Random chance of 1 in 20 of not being authenticated
    if random.randint(1, 20) == 1:
        return None

    user_data = check_redis(user_id)
    if not user_data:
        user_data = get_member(user_id)
        if user_data:
            load_redis(user_data)
    return user_data


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


@app.route("/login", methods=["POST"])
def login():
    return_code = 200
    component = 'login'
    transaction_id = None
    try:
        data = request.get_json()
        user_id = data.get("userId", None)
        payload = {
            'userId': user_id,
        }
        transaction_id = request_log(component, payload)
        if not user_id:
            return_code = 400
        else:
            url = get_env_variable("AUTHENTICATION_URL")
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                payload = response.json()
            else:
                return_code = response.status_code
                payload = {"error": "Error from authenticate service", "returnCode": response.status_code}
    except Exception as ex:
        return_code = 500
        payload = {"error": INTERNAL_ERROR, "details": str(ex)}
    response_log(transaction_id, component, return_code, payload)
    return make_response(jsonify(payload), return_code)


@app.route("/authenticate", methods=["POST"])
def authenticate():
    return_code = 200
    component = 'authenticate'
    transaction_id = None
    try:
        data = request.get_json()
        user_id = data.get("userId", None)
        payload = {
            'userId': user_id,
        }
        transaction_id = request_log(component, payload)
        if not user_id:
            return_code = 400
        else:
            user = authenticate_user(user_id)
            if not user:
                return_code = 401
            else:
                payload = user
    except Exception as ex:
        return_code = 500
        payload = {"error": INTERNAL_ERROR, "details": str(ex)}
    response_log(transaction_id, component, return_code, payload)
    return make_response(jsonify(payload), return_code)


@app.route("/member", methods=["POST"])
def member():
    return_code = 200
    component = 'member'
    transaction_id = None
    try:
        data = request.get_json()
        user_id = data.get("userId", None)
        payload = {
            'userId': user_id,
        }
        transaction_id = request_log(component, payload)
        if not user_id:
            return_code = 400
        else:
            user = member_service(user_id)
            if not user:
                return_code = 401
            else:
                payload = user
    except Exception as ex:
        return_code = 500
        payload = {"error": INTERNAL_ERROR, "details": str(ex)}
    response_log(transaction_id, component, return_code, payload)
    return make_response(jsonify(payload), return_code)

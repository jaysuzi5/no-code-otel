from flask import Flask, jsonify
import psycopg2
from confluent_kafka import Producer
import logging
import os
import random
import uuid
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from datetime import datetime
from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor
from elasticsearch import Elasticsearch

ElasticsearchInstrumentor().instrument()
ConfluentKafkaInstrumentor().instrument()

app = Flask(__name__)


def get_env_variable(var_name, default=None):
    """
    Gets an environment variable.

    Args:
        var_name (str): Name of the environment variable.
        default (any, optional): Default value if the variable is not found.

    Returns:
        any: The value of the environment variable, or the default if not found.

    Raises:
        ValueError: If the environment variable is not found and no default
                    value is provided.
    """
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        else:
            raise ValueError(f"Environment variable '{var_name}' not set.")
    return value


def connect_to_database():
    """Connects to the PostgreSQL database."""
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
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

def publish_to_elastic(records):
    try:
        es = Elasticsearch("http://elasticsearch-master.elasticsearch.svc.cluster.local:9200")

        document = {
            "id": str(uuid.uuid4()),
            "records": records,
            "content": "Weather Data Pulled From Database",
            "timestamp": datetime.utcnow().isoformat()  # or use datetime.now() for local time
        }

        # Index the document
        response = es.index(index="weather-inquires", id=document["id"], document=document)
        print(response)
    except Exception as ex:
        logging.error(f"Error publishing to Elasticsearch: {ex}")


def publish_event(records):
    conf = {
        'bootstrap.servers': 'kafka.kafka.svc.cluster.local:9092'
    }
    producer = Producer(conf)
    topic = "test"

    def delivery_report(err, msg):
        if err is not None:
            logging.error(f"Delivery failed: {err}")
        else:
            logging.info(f"Delivered message to {msg.topic()} [{msg.partition()}]")

    message = {
        "id": str(uuid.uuid4()),
        "records": records,
        "content": "Weather Data Pulled From Database"
    }
    producer.produce(topic, value=str(message), callback=delivery_report)
    producer.poll(0)

    producer.flush()


def get_latest_weather():
    try:
        n = random.randint(-50, 1050)
        conn = connect_to_database()
        cur = conn.cursor()

        cur.execute("SELECT * FROM get_latest_weather(%s)", (n,))
        results = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        records = len(results)
        publish_event(records)
        publish_to_elastic(records)
        return [dict(zip(column_names, row)) for row in results]
    finally:
        cur.close()
        conn.close()


@app.route("/latest-weather")
def latest_weather():
    logging.info("Received request for latest weather.")
    return jsonify(get_latest_weather())

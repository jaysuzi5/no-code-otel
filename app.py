from flask import Flask, jsonify
import psycopg2
import logging
import os

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


def get_latest_weather():
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute("SELECT * FROM weather_current ORDER BY collection_time DESC LIMIT 1;")
    row = cur.fetchone()
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return dict(zip(column_names, row))


@app.route("/latest-weather")
def latest_weather():
    return jsonify(get_latest_weather())

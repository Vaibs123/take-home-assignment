import logging
import os
import psycopg2
import sys

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel("INFO")

load_dotenv()


def get_conn():
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_database = os.getenv("POSTGRES_DB")
    try:
        conn = psycopg2.connect(
            database=postgres_database, 
            user=postgres_user, 
            password=postgres_password, 
            host='localhost', 
            port='5432'
        )
        logger.info("Connection successful")
    except:
        logger.error("Error connecting to the database.")
        raise
    return conn


def create_cursor(conn):
    cursor = conn.cursor()
    return cursor


def connect_to_database():
    conn = get_conn()
    cursor = create_cursor(conn)
    return conn, cursor


def close_connection(conn):
    conn.close()
    logger.info("Connection closed")


def execute_command(conn, cursor, sql):
    cursor.execute(sql)
    conn.commit()


def get_data(cursor, sql):
    cursor.execute(sql)
    return cursor


def insert_command(conn, cursor, table_name, column_names, output_csv):
    copy = f"COPY {table_name}({','.join(column_names)}) FROM STDIN with csv"
    with open(output_csv) as csvFile:
        next(csvFile) # skip headers
        cursor.copy_expert(sql=copy, file=csvFile)
    conn.commit()

import glob
import logging
import pandas as pd
import sys

from pathlib import Path

import postgres_operations
import sql_operations

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel("INFO")

table_name = 'crime'
input_csv = Path(f"data/{table_name}.csv")
output_csv = Path(f"data/output_{table_name}.csv")
column_names = [
    "unique_key",
    "case_number",
    "date",
    "block",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "beat",
    "district",
    "ward",
    "community_area",
    "year",
    "latitude",
    "longitude",
]


def extract():
    logger.info("Extracting data")
    data = pd.read_csv(input_csv, usecols = column_names)
    data['date'] = pd.to_datetime(data['date'], utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
    data['beat'] = pd.to_numeric(data['beat'], errors='coerce').astype('Int64')
    data['district'] = pd.to_numeric(data['district'], errors='coerce').astype('Int64')
    data['ward'] = pd.to_numeric(data['ward'], errors='coerce').astype('Int64')
    data['community_area'] = pd.to_numeric(data['community_area'], errors='coerce').astype('Int64')
    data.to_csv(output_csv, index=False)
    logger.info("Extracting data successful")


def connect_to_database():
    conn = postgres_operations.get_conn()
    cursor = postgres_operations.create_cursor(conn)
    return conn, cursor


def run_select_sql(cursor):
    select_files = glob.glob("sql/select_*")
    for file in select_files:
        select_query = open(file, "r").read()
        sql_operations.select_data(cursor, select_query)


def load_data(conn, cursor):
    drop_table_sql = open("sql\drop_table.sql", "r").read()
    create_table_sql = open("sql\create_table.sql", "r").read()
    sql_operations.drop_table(conn, cursor, drop_table_sql)
    sql_operations.create_table(conn, cursor, create_table_sql)
    sql_operations.insert_data(conn, cursor, table_name, column_names, output_csv)


if __name__ == "__main__":
    extract()
    conn, cursor = connect_to_database()
    load_data(conn, cursor)
    run_select_sql(cursor)
    postgres_operations.close_connection(conn)

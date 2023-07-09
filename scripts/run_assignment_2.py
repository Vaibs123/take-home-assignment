import glob
import logging
import json
import pandas as pd
import sys

from pathlib import Path

import postgres_operations
import sql_operations

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel("INFO")

table_name = "crime"
input_csv = Path(f"data/{table_name}.csv")
output_csv = Path(f"data/output_{table_name}.csv")
table_schema_path = Path(f"schema/{table_name}.json")


def get_columns(table_schema) -> list:
    column_names = list(table_schema.keys())
    return column_names


def get_table_schema(table_schema_path) -> json:
    f = open(table_schema_path)
    data = json.load(f)
    return data


def create_table_sql(table_schema) -> str:
    all_columns = []
    pk_list = []
    for columns, constraints_dict in table_schema.items():
        column_details = [
            values
            for constraints, values in constraints_dict.items()
            if constraints != "primary_key"
        ]
        column_details = " ".join(column_details)
        all_columns.extend([columns + " " + column_details])

        if "primary_key" in constraints_dict:
            pk_list.append(columns)

    if pk_list:
        pk_constraint = f", primary key ({', '.join(pk_list)})"
        create_sql = ", ".join(all_columns) + pk_constraint
    else:
        create_sql = ", ".join(all_columns)

    create_sql = f"create table {table_name} ({create_sql});"
    logger.info("Table sql created")
    return create_sql


def _extract(column_names):
    logger.info("Extracting data")
    data = pd.read_csv(input_csv, usecols=column_names)
    data["date"] = pd.to_datetime(data["date"], utc=True).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    data["beat"] = pd.to_numeric(data["beat"], errors="coerce").astype("Int64")
    data["district"] = pd.to_numeric(data["district"], errors="coerce").astype("Int64")
    data["ward"] = pd.to_numeric(data["ward"], errors="coerce").astype("Int64")
    data["community_area"] = pd.to_numeric(
        data["community_area"], errors="coerce"
    ).astype("Int64")
    data.to_csv(output_csv, index=False)
    logger.info("Extracting data successful")


def _run_select_sql(cursor):
    select_files = glob.glob("sql/select_*")
    for file in select_files:
        select_query = open(file, "r").read()
        sql_operations.select_data(cursor, select_query)


def _load_data(conn, cursor, column_names, table_schema):
    drop_table_sql = open("sql\drop_table.sql", "r").read()
    create_query = create_table_sql(table_schema)
    sql_operations.drop_table(conn, cursor, drop_table_sql)
    sql_operations.create_table(conn, cursor, create_query)
    sql_operations.insert_data(conn, cursor, table_name, column_names, output_csv)


if __name__ == "__main__":
    table_schema = get_table_schema(table_schema_path)
    column_names = get_columns(table_schema)
    _extract(column_names)
    conn, cursor = postgres_operations.connect_to_database()
    _load_data(conn, cursor, column_names, table_schema)
    _run_select_sql(cursor)
    postgres_operations.close_connection(conn)

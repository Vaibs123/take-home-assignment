import logging
import sys
import pandas as pd
import postgres_operations

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel("INFO")


def drop_table(conn, cursor, drop_table_sql):
    try:
        postgres_operations.execute_command(conn, cursor, drop_table_sql)
        logger.info("Dropped the table successfully")
    except:
        logger.error("Error dropping the table. Rolling back the connection")
        conn.rollback()
        raise


def create_table(conn, cursor, create_table_sql):
    try:
        postgres_operations.execute_command(conn, cursor, create_table_sql)
        logger.info("Created the table successfully")
    except:
        logger.error("Error creating the table. Rolling back the connection")
        conn.rollback()
        raise


def insert_data(conn, cursor, table_name, column_names, output_csv):
    try:
        postgres_operations.insert_command(
            conn, cursor, table_name, column_names, output_csv
        )
        logger.info("Inserted the data successfully")
    except:
        logger.error("Error inserting the data. Rolling back the connection")
        conn.rollback()
        raise


def select_data(cursor, select_query):
    cursor = postgres_operations.get_data(cursor, select_query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(result, columns=columns)
    print(result)

# ConnectDatabase file
import duckdb
import os
from contextlib import contextmanager
import logging

def connect():
    """Establish connection to the database"""
    conn = duckdb.connect('my_database.db', read_only=True)
    print("Connected to the database")
    return conn
def connect_payments():
    """Establish connection to the database"""
    conn = duckdb.connect('payments.db', read_only=True)
    print("Connected to the database")
    return conn

def connect_write():
    """Establish connection to the database"""
    conn = duckdb.connect('my_database.db', read_only=False)
    print("Connected to the database")
    return conn
def connect_write_payments():
    """Establish connection to the database"""
    conn = duckdb.connect('payments.db', read_only=False)
    print("Connected to the database")
    return conn

def reset(conn,file_path,table_name):
    """Drop all tables using existing connection"""
    try:
        # Get list of all tables
        tables = conn.sql("SHOW TABLES").fetchall()

        # Drop each table
        for table in tables:
            table_name = table[0]
            conn.sql(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(f"Dropped table: {table}")
        print("All tables have been dropped successfully")

        # Import CSV file into new table
        conn.sql(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto({file_path})
        """)
        print(f"Successfully imported {file_path} into table {table_name}")

        print("\nFirst 5 rows of the sales table:")
        conn.sql(f"SELECT * FROM {table_name} LIMIT 5").show()
    except Exception as e:
        print(f"Error occurred: {e}")
        raise





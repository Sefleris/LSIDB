# ConnectDatabase file
import duckdb
import os
from contextlib import contextmanager
import logging

def connect():
    """Establish connection to the database"""
    conn = duckdb.connect('my_database.db')
    print("Connected to the database")
    return conn


def reset(conn):
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
        conn.sql("""
            CREATE TABLE sales AS 
            SELECT * FROM read_csv_auto('supermarket_sales.csv')
        """)
        print("Successfully imported supermarket_sales.csv into table sales")

        print("\nFirst 5 rows of the sales table:")
        conn.sql("SELECT * FROM sales LIMIT 5").show()
    except Exception as e:
        print(f"Error occurred: {e}")
        raise

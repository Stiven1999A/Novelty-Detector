"""database/connections.py"""
import os
import pyodbc
from sqlalchemy import create_engine

def connect_to_insert_data():
    """Connect to SQL Server database

    Returns:
        pyodbc.Connection: Database connection object
    """
    db_name = os.getenv("DB_NAME_INSERTIONS")
    user = os.getenv("DB_USER_INSERTIONS")
    password = os.getenv("DB_PASSWORD_INSERTIONS")
    host = os.getenv("DB_SERVER_INSERTIONS")
    port = os.getenv("DB_PORT_INSERTIONS")

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={db_name};"
        f"UID={user};"
        f"PWD={password};"
        f"Timeout=60;"
    )
    conn = pyodbc.connect(conn_str)
    if conn:
        print("Connection to the database for inserting data was successful.")
    return conn

def connect_to_fetch_data():
    """
    Establishes a connection to the specified SQL Server database for fetching data.

    Returns:
        sqlalchemy.engine.base.Connection: A connection object to the database.
    """
    db_server = os.getenv("DB_SERVER_EXTRACTION")
    db_name = os.getenv("DB_NAME_EXTRACTION")
    db_user = os.getenv("DB_USER_EXTRACTION")
    db_password = os.getenv("DB_PASSWORD_EXTRACTION")
    db_driver = os.getenv("DB_DRIVER_EXTRACTION")
    conn_str = (
        f"mssql+pyodbc://{db_user}:{db_password}@{db_server}/{db_name}"
        f"?driver={db_driver}&timeout=60"
    )
    engine = create_engine(conn_str)
    connection = engine.connect()
    if connection:
        print("Connection to the database for fetching data was successful.")
    return connection

def connect_to_insert_forecasting_data():
    """Connect to SQL Server database using SQLAlchemy engine

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine object
    """
    db_name = os.getenv("DB_NAME_INSERTIONS")
    user = os.getenv("DB_USER_INSERTIONS")
    password = os.getenv("DB_PASSWORD_INSERTIONS")
    host = os.getenv("DB_SERVER_INSERTIONS")
    port = os.getenv("DB_PORT_INSERTIONS")

    conn_str = (
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{db_name}"
        f"?driver=ODBC+Driver+17+for+SQL+Server&timeout=60"
    )
    engine = create_engine(conn_str)
    connection = engine.connect()
    if connection:
        print("Connection to the database for inserting data was successful.")
    return connection

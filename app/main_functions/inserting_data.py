"""DETECTOR-DE-NOVEDADES/main_functions/inserting_data.py"""
import pandas as pd
from database_tools.create_tables import create_tables
from database_tools.delete_tables import delete_tables
from database_tools.update_tables import (
    update_processes,
    update_groups,
    update_procesos_grupos,
    update_fechas,
    add_day_of_week_id,
    filter_existing_rows
)
def check_tables_exist(conn):
    """
    Checks if tables exist in the specified database schema and catalog.

    This function queries the information schema to determine if any tables
    exist within the 'dbo' schema of the 'Consumos-PrediccionesMIPS' catalog.
    If no tables are found, it calls the `create_tables` function to create them.

    Args:
        conn (pyodbc.Connection): A connection object to the database.

    Returns:
        str: A message indicating whether the tables exist or not.
    """
    print("Checking if tables exist...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'dbo' AND table_catalog = 'Consumos-PrediccionesMIPS';
    """)
    count = cursor.fetchone()[0]
    cursor.close()
    conn.commit()
    print(f"{count} tables exist." if count > 0 else "Tables do not exist.")
    if count == 0:
        print("Tables do not exist.")
        create_tables(conn)
    elif count == 10:
        print("Tables are already created.")
    elif count < 10:
        print("Some tables are missing.")
        delete_tables(conn)
        create_tables(conn)
    else:
        print("Unexpected number of tables. More than 10 tables exist.")

def fetch_new_data(conn_insert, conn_fetch):
    """
    Fetches new data from the specified SQL Server database using the fetch connection,
    and processes it using the insert connection.

    Args:
        conn_insert: A connection object to the database for inserting data.
        conn_fetch: A connection object to the database for fetching data.

    Returns:
        pd.DataFrame: A DataFrame containing the results of the executed SQL query.
    """
    print("Fetching new data...")
    cursor = conn_insert.cursor()
    cursor.execute('SELECT COUNT(*) FROM dbo.ConsumosMIPS')
    count = cursor.fetchone()[0]

    if count == 0:
        query = ("""SELECT * FROM dbo.refrescarprocesos_10dias Where Fecha <= '2024-10-31';""")

    else:
        cursor.execute('SELECT MAX(IdFecha) FROM dbo.ConsumosMIPS')
        last_id_fecha = cursor.fetchone()[0]
        cursor.execute(f'SELECT Fecha FROM dbo.Fechas WHERE IdFecha = {last_id_fecha}')
        last_date = cursor.fetchone()[0]
        query = (f"""SELECT * FROM dbo.refrescarprocesos_10dias WHERE Fecha > '{last_date}';""")

    df = pd.read_sql(query, conn_fetch)
    
    if df.empty:
        print("Data is already updated with the last data available.")
        return df
    
    return df

def update_database(conn, df):
    """
    Updates the database with the provided DataFrame.

    Args:
        conn: A database connection object.
        df: A pandas DataFrame containing the data to be updated.

    Returns:
        pd.DataFrame: A DataFrame with the updated data.
    """
    if df.empty:
        return df
    
    df = add_day_of_week_id(df)
    df = update_processes(conn, df)
    df = update_groups(conn, df)
    df = update_fechas(conn, df)
    update_procesos_grupos(conn, df)
    df = filter_existing_rows(df, conn)
    
    return df

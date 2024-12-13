"""DETECTOR-DE-NOVEDADES/main.py"""
import pyodbc
import time
from dotenv import load_dotenv
from scripts.insertingdata import (
    check_tables_exist,
    fetch_new_data,
    update_database
)
from scripts.forecasting import predictions_orchestrator
from database_tools.connections import (
    connect_to_insert_data, 
    connect_to_fetch_data, 
    connect_to_insert_forecasting_data
)

load_dotenv()

def main():
    """
    Main function to update consumption data and execute forecasting.

    This function performs the following steps:
    1. Creates connections to the databases for inserting data, fetching data, and inserting forecasting data.
    2. Updates the consumption data by checking if the necessary tables exist, fetching new data, and updating the database.
    3. Executes the forecasting process using the updated data.

    If a database or file error occurs, it catches the exception and prints an error message.

    Finally, it ensures that all database connections are closed.

    Raises:
        pyodbc.DatabaseError: If a database error occurs.
        FileNotFoundError: If a file-related error occurs.
    """
    while True:
        try:
            print("Updating Consumption Data...")
            conn_insert = connect_to_insert_data()
            conn_fetch = connect_to_fetch_data()
            conn_insert_predictions = connect_to_insert_forecasting_data()
            check_tables_exist(conn_insert)
            new_data = fetch_new_data(conn_insert, conn_fetch)
            updated_data = update_database(conn_insert, new_data)

            if updated_data.empty:
                print("No new data to update")
                break

            print("Data updated successfully")
            print("")
            print("Executing Forecasting...")
            predictions_orchestrator(conn_insert, conn_insert_predictions)
            print("Forecasting executed successfully")
            break
        except (pyodbc.DatabaseError, FileNotFoundError) as e:
            print(f"A database or file error occurred: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
        finally:
            try:
                conn_insert.close()
                conn_fetch.close()
                conn_insert_predictions.close()
            except NameError:
                pass

if __name__ == "__main__":
    main()

"DETECTOR-DE-NOVEDADES/scripts/forecasting.py"
import pandas as pd
from prophet import Prophet
from database_tools.update_tables import add_day_of_week_id
from forecast_tools.metrics import metrics
from sqlalchemy.exc import OperationalError, PendingRollbackError
from database_tools.connections import connect_to_insert_forecasting_data, connect_to_insert_data

def parameters(conn):
    """
    Retrieves parameters from the database.

    This function connects to the database using the provided connection object,
    and fetches the following parameters:
    - The count of predictions from the table `dbo.PrediccionesMIPS`.
    - The maximum `IdFecha` from the table `dbo.ConsumosMIPS`.
    - The minimum `IdFecha` from the table `dbo.PrediccionesMIPS`.

    Args:
        conn (object): A database connection object.

    Returns:
        tuple: A tuple containing:
            - predictions_count (int): The count of predictions.
            - min_id_fecha (int): The minimum `IdFecha` from `dbo.PrediccionesMIPS`.
            - max_id_fecha (int): The maximum `IdFecha` from `dbo.ConsumosMIPS`.
    """
    print("Finding Parameters...")
    cursor = conn.cursor()

    #Fetching count of predictions from dbo.PrediccionesMIPS
    cursor.execute('SELECT COUNT(*) FROM dbo.PrediccionesMIPS;')
    predictions_count = cursor.fetchone()[0] or 0

    #Fetching max_id_fecha from dbo.ConsumosMIPS
    cursor.execute('SELECT MAX(IdFecha) FROM dbo.ConsumosMIPS;')
    max_id_fecha = cursor.fetchone()[0]

    #Fetching min_id_fecha from dbo.PrediccionesMIPS
    cursor.execute('SELECT MIN(IdFecha) FROM dbo.PrediccionesMIPS;')
    min_id_fecha = cursor.fetchone()[0] or max_id_fecha

    return predictions_count, min_id_fecha, max_id_fecha

def forecast_and_insert(max_id_fecha, conn, engine):
    """
    Forecasts future values of ConsumoMIPS and inserts the predictions into the database.
    Parameters:
    max_id_fecha (int): The maximum IdFecha to consider for fetching historical data.
    conn (pyodbc.Connection): The database connection object.
    engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object for executing SQL queries.
    Returns:
    None
    This function performs the following steps:
    1. Fetches historical ConsumoMIPS data up to the specified max_id_fecha.
    2. Fetches corresponding dates for the historical data.
    3. Prepares the data for the Prophet forecasting model.
    4. Fits the Prophet model to the historical data.
    5. Fetches future dates for prediction.
    6. Predicts future ConsumoMIPS values using the fitted Prophet model.
    7. Adjusts the first row of the forecast to match the last known historical value.
    8. Merges the forecast with future dates and additional information.
    9. Inserts the forecasted values into the database in bulk.
    10. Handles any exceptions that occur during the process and rolls back the transaction if necessary.
    """
    print("Forecasting and Inserting...")
    cursor = conn.cursor()
    try:
        cursor.execute("""ALTER SEQUENCE predicciones_seq
                       RESTART WITH 1
                       INCREMENT BY 1
                       MINVALUE 1
                       MAXVALUE 100000
                       CYCLE;""")
        print(f"Fetching data from ConsumosMIPS")
        query = f"""
            SELECT IdFecha, SUM(ConsumoMIPS) as ConsumoMIPS FROM dbo.ConsumosMIPS
            WHERE IdFecha <= {max_id_fecha}
            GROUP BY IdFecha;
            """
        data = pd.read_sql(query, engine)
        print("data fetched successfully")
        
        # Add the Fecha column to the data
        date_query = f"""
        SELECT IdFecha, Fecha FROM dbo.Fechas
        WHERE IdFecha <= {max_id_fecha};
        """
        date_data = pd.read_sql(date_query, engine)
        data = data.merge(date_data, on='IdFecha', how='left')
        
        # Preparing the data for the Prophet model
        data['Fecha'] = pd.to_datetime(data['Fecha'], format='%Y-%m-%d')
        # Sort the data by IdFecha in ascending order
        data = data.sort_values(by='IdFecha')
        print(data.tail())
        prophet_df = data[['Fecha', 'ConsumoMIPS']].rename(columns={'Fecha': 'ds', 'ConsumoMIPS': 'y'})
        print(prophet_df)
        
        # Fitting the Prophet model
        model = Prophet()
        model.add_country_holidays(country_name='CO')
        model.fit(prophet_df)

        # Predicting the future values
        future_dates_query = f"""
        SELECT IdFecha, Fecha as ds FROM dbo.Fechas
        WHERE IdFecha >= {max_id_fecha};
        """
        future_dates = pd.read_sql(future_dates_query, engine)

        # Predicting the future values
        print(f"Forecasting")
        forecast = model.predict(future_dates)
        forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        forecast = forecast.rename(columns={'ds': 'Fecha', 'yhat': 'Prediccion', 'yhat_lower': 'LimInf', 'yhat_upper': 'LimSup'})
        
        # Replace the values of the first row in the columns Prediccion, LimInf, and LimSup
        forecast.at[0, 'Prediccion'] = data['ConsumoMIPS'].iloc[-1]
        forecast.at[0, 'LimInf'] = data['ConsumoMIPS'].iloc[-1]
        forecast.at[0, 'LimSup'] = data['ConsumoMIPS'].iloc[-1]

        # Merge the forecast with the future_dates dataframe
        future_dates['ds'] = pd.to_datetime(future_dates['ds'], format='%Y-%m-%d')
        future_dates = future_dates.rename(columns={'ds': 'Fecha'})
        future_dates = add_day_of_week_id(future_dates)
        forecast = forecast.merge(future_dates, on='Fecha', how='left')
        
        # Add IdPrediccion column using the sequence predicciones_seq
        forecast['IdPrediccion'] = [cursor.execute("SELECT NEXT VALUE FOR predicciones_seq").fetchone()[0] for _ in range(len(forecast))]

        # Reorder the columns to match the table structure
        forecast = forecast[['IdPrediccion', 'IdFecha', 'IdDiaSemana', 'Prediccion', 'LimInf', 'LimSup']]
        print(forecast.head())  
        # Inserting forecast into the database in bulk
        print(f"Inserting forecast into the database")
        forecast_to_insert = [
            (
                int(row['IdPrediccion']),
                int(row['IdFecha']),
                int(row['IdDiaSemana']),
                float(row['Prediccion']),
                float(row['LimInf']),
                float(row['LimSup'])
            )
            for _, row in forecast.iterrows()
        ]
        cursor.executemany("""
            INSERT INTO dbo.PrediccionesMIPS (IdPrediccion, IdFecha, IdDiaSemana, Prediccion, LimInf, LimSup)
            VALUES (?, ?, ?, ?, ?, ?)
        """, forecast_to_insert)
        conn.commit()
        print("Forecast inserted successfully")

    except OperationalError as e:
        print(f"OperationalError: {e}")
        conn.rollback()
    except PendingRollbackError as e:
        print(f"PendingRollbackError: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error in forecast_and_insert: {e}")
        conn.rollback()

    return

def calculate_metrics(min_id_fecha, max_id_fecha, conn):
    """
    Calculate and insert various forecasting metrics into the MetricasPredicciones table.
    This function calculates metrics such as MAE, MSE, RMSE, MAPE, and sMAPE for a range of dates
    and inserts them into the MetricasPredicciones table in the database. It handles both the cases
    where the table is initially empty and where it already contains data.
    Parameters:
    min_id_fecha (int): The minimum IdFecha value to start calculating metrics from.
    max_id_fecha (int): The maximum IdFecha value to calculate metrics up to.
    conn (pyodbc.Connection): The database connection object.
    Returns:
    None
    """
    """"""
    print("Calculating Metrics...")
    cursor = conn.cursor()

    # Check if the MetricasPredicciones table is empty
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS count FROM dbo.MetricasPredicciones;")
    metrics_count = cursor.fetchone()[0]

    if metrics_count == 0:
        print("The MetricasPredicciones table is empty.")

        metric_categories = 2

        cursor.execute("""ALTER SEQUENCE metricas_seq 
        RESTART WITH 1
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 100000
        CYCLE;""")

        cursor.execute("SELECT NEXT VALUE FOR metricas_seq")
        id_metrica = int(cursor.fetchone()[0])  

        n = 1

        last_metrics = {
            "MAE": 0,
            "MSE": 0,
            "RMSE": 0,
            "MAPE": 0,
            "sMAPE": 0
        }

        for id_fecha in range(min_id_fecha + 1, max_id_fecha + 1):
            print(f"Calculating metrics for IdFecha: {id_fecha}")
            print(f"This is the id_metrica: {id_metrica}")

            # Fetch y_true and y_pred
            cursor.execute(f"SELECT SUM(ConsumoMIPS) FROM dbo.ConsumosMIPS WHERE IdFecha = {id_fecha};")
            y_true = cursor.fetchone()[0]
            print(f"y_true: {y_true}")

            cursor.execute(f"SELECT Prediccion FROM dbo.PrediccionesMIPS WHERE IdFecha = {id_fecha};")
            y_pred = cursor.fetchone()[0]
            print(f"y_pred: {y_pred}")

            metrics_result = metrics(n=n, y_true=y_true, y_pred=y_pred, last_metrics=last_metrics)

            for category in range(metric_categories):

                print("Inserting metrics...")
                cursor.execute("""
                    INSERT INTO dbo.MetricasPredicciones 
                    (IdMetrica, IdFecha, IdCategoriaMetrica, MAE, MSE, RMSE, MAPE, sMAPE)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    id_metrica,
                    id_fecha,
                    category,
                    float(metrics_result['MAE']),
                    float(metrics_result['MSE']),
                    float(metrics_result['RMSE']),
                    float(metrics_result['MAPE']),
                    float(metrics_result['sMAPE'])
                ))
                conn.commit()
                if id_fecha < max_id_fecha + 1:
                    cursor.execute("SELECT NEXT VALUE FOR metricas_seq")
                    id_metrica = cursor.fetchone()[0]
                print(id_metrica)

            n += 1
            last_metrics = metrics_result

    else:
        print("The MetricasPredicciones table is not empty.")
        cursor.execute("SELECT NEXT VALUE FOR metricas_seq")
        id_metrica = int(cursor.fetchone()[0])

        for id_fecha in range(min_id_fecha + 1, max_id_fecha + 1):

            cursor.execute(f"""
                           SELECT MONTH(Fecha), DAY(Fecha) FROM dbo.Fechas
                           WHERE IdFecha BETWEEN {id_fecha - 1} AND {id_fecha};
                           """)
            month_day = cursor.fetchall()

            if month_day[0][0] != month_day[1][0]:
                n = 1
                last_metrics = {
                    "MAE": 0,
                    "MSE": 0,
                    "RMSE": 0,
                    "MAPE": 0,
                    "sMAPE": 0
                }

                # Fetch y_true and y_pred
                cursor.execute(f"SELECT SUM(ConsumoMIPS) FROM dbo.ConsumosMIPS WHERE IdFecha = {id_fecha};")
                y_true = cursor.fetchone()[0]

                cursor.execute(f"SELECT SUM(Prediccion) FROM dbo.PrediccionesMIPS WHERE IdFecha = {id_fecha};")
                y_pred = cursor.fetchone()[0]

                metrics_result = metrics(n=n, y_true=y_true, y_pred=y_pred, last_metrics=last_metrics)

                print("Inserting metrics...")
                cursor.execute("""
                    INSERT INTO dbo.MetricasPredicciones 
                    (IdMetrica, IdFecha, IdCategoriaMetrica, MAE, MSE, RMSE, MAPE, sMAPE)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    id_metrica,
                    id_fecha,
                    0,
                    float(metrics_result['MAE']),
                    float(metrics_result['MSE']),
                    float(metrics_result['RMSE']),
                    float(metrics_result['MAPE']),
                    float(metrics_result['sMAPE'])
                ))
                conn.commit()
                
                if id_fecha < max_id_fecha + 1:
                    cursor.execute("SELECT NEXT VALUE FOR metricas_seq")
                    id_metrica = cursor.fetchone()[0]
                
            else:
                # We must find the last n value
                n = int(month_day[1][1])

                cursor.execute(f"""SELECT * FROM dbo.MetricasPredicciones
                               WHERE IdFecha = {id_fecha - 1}
                               AND IdCategoriaMetrica = 0;
                               """)
                last_metrics = cursor.fetchall()

                last_metrics = {
                    "MAE": last_metrics[0][3],
                    "MSE": last_metrics[0][4],
                    "RMSE": last_metrics[0][5],
                    "MAPE": last_metrics[0][6],
                    "sMAPE": last_metrics[0][7]
                }

                # Fetch y_true and y_pred
                cursor.execute(f"SELECT SUM(ConsumoMIPS) FROM dbo.ConsumosMIPS WHERE IdFecha = {id_fecha};")
                y_true = cursor.fetchone()[0]
                print(f"y_true: {y_true}")

                cursor.execute(f"SELECT SUM(Prediccion) FROM dbo.PrediccionesMIPS WHERE IdFecha = {id_fecha};")
                y_pred = cursor.fetchone()[0]
                print(f"y_pred: {y_pred}")

                metrics_result = metrics(n=n, y_true=y_true, y_pred=y_pred, last_metrics=last_metrics)

                print("Inserting metrics...")
                cursor.execute("""
                    INSERT INTO dbo.MetricasPredicciones 
                    (IdMetrica, IdFecha, IdCategoriaMetrica, MAE, MSE, RMSE, MAPE, sMAPE)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    id_metrica,
                    id_fecha,
                    0,
                    float(metrics_result['MAE']),
                    float(metrics_result['MSE']),
                    float(metrics_result['RMSE']),
                    float(metrics_result['MAPE']),
                    float(metrics_result['sMAPE'])
                ))
                conn.commit()
                if id_fecha < max_id_fecha + 1:
                    cursor.execute("SELECT NEXT VALUE FOR metricas_seq")
                    id_metrica = cursor.fetchone()[0]
            # Inserting the metrics to the category 1

            cursor.execute("""
                           SELECT COUNT(*) FROM dbo.MetricasPredicciones
                           WHERE IdCategoriaMetrica = 1;
                           """)
            n = int(cursor.fetchone()[0]) + 1

            cursor.execute(f"""
                           SELECT * FROM dbo.MetricasPredicciones
                           WHERE IdFecha = {id_fecha - 1}
                           AND IdCategoriaMetrica = 1;
                           """)
            last_metrics = cursor.fetchall()
            last_metrics = {
                "MAE": last_metrics[0][3],
                "MSE": last_metrics[0][4],
                "RMSE": last_metrics[0][5],
                "MAPE": last_metrics[0][6],
                "sMAPE": last_metrics[0][7]
            }

            cursor.execute(f"SELECT SUM(ConsumoMIPS) FROM dbo.ConsumosMIPS WHERE IdFecha = {id_fecha};")
            y_true = cursor.fetchone()[0]
            print(f"y_true: {y_true}")

            cursor.execute(f"SELECT SUM(Prediccion) FROM dbo.PrediccionesMIPS WHERE IdFecha = {id_fecha};")
            y_pred = cursor.fetchone()[0]
            print(f"y_pred: {y_pred}")

            metrics_result = metrics(n=n, y_true=y_true, y_pred=y_pred, last_metrics=last_metrics)

            print("Inserting metrics...")
            cursor.execute("""
                INSERT INTO dbo.MetricasPredicciones 
                (IdMetrica, IdFecha, IdCategoriaMetrica, MAE, MSE, RMSE, MAPE, sMAPE)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_metrica,
                id_fecha,
                1,
                float(metrics_result['MAE']),
                float(metrics_result['MSE']),
                float(metrics_result['RMSE']),
                float(metrics_result['MAPE']),
                float(metrics_result['sMAPE'])
            ))
            conn.commit()
            
            if id_fecha < max_id_fecha + 1:
                cursor.execute("SELECT NEXT VALUE FOR metricas_seq")
                id_metrica = cursor.fetchone()[0]

    return print("Metrics calculated successfully")
    
def predictions_orchestrator(conn, engine):
    """
    Orchestrates the prediction process by either resetting the prediction sequence
    and generating new forecasts or calculating metrics and generating forecasts based
    on the existing data.
    Args:
        conn (psycopg2.extensions.connection): The connection object to the PostgreSQL database.
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object for database operations.
    Returns:
        None
    Side Effects:
        - Executes SQL commands to reset the prediction sequence if no predictions exist.
        - Calls the `forecast_and_insert` function to generate and insert new forecasts.
        - Calls the `calculate_metrics` function to compute metrics if predictions exist.
    """
    print("Predictive Model Executed")
    
    #Calling the parameters function
    predictions_count, min_id_fecha, max_id_fecha = parameters(conn)

    #Creating a cursor object
    cursor = conn.cursor()

    if predictions_count == 0:
        return forecast_and_insert(max_id_fecha, conn, engine)

    else:
        calculate_metrics(min_id_fecha, max_id_fecha, conn)
        cursor.execute("""DELETE FROM dbo.PrediccionesMIPS;""")
        return forecast_and_insert(max_id_fecha, conn, engine)

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
            print("Connecting to databases...")
            conn_insert = connect_to_insert_data()
            conn_insert_predictions = connect_to_insert_forecasting_data()
            print("Executing Forecasting...")
            predictions_orchestrator(conn_insert, conn_insert_predictions)
            print("Forecasting executed successfully")
            break
        finally:
            try:
                conn_insert.close()
                conn_insert_predictions.close()
            except NameError:
                pass

if __name__ == "__main__":
    main()
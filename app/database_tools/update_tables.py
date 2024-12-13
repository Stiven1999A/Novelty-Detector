"""DETECTOR-DE-NOVEDADES/database/dataframe_utils.py"""
import pandas as pd
import numpy as np
import time

def update_processes(conn, df):
    """Update processes in the database and update the input DataFrame with process IDs.

    Args:
        conn (pyodbc.Connection): Database connection.
        df (pd.DataFrame): Input DataFrame containing 'NombreProceso' column.

    Returns:
        pd.DataFrame: Updated DataFrame with 'IdProceso' column.
    """
    print("Updating the Procesos table.")
    cursor = conn.cursor()
    cursor.execute('SELECT IdProceso, NombreProceso FROM dbo.Procesos')
    existing_processes = cursor.fetchall()
    existing_processes_dict = {row[1]: row[0] for row in existing_processes}
    nombres_proceso_unicos = df['NombreProceso'].unique()
    new_processes = [proc for proc in nombres_proceso_unicos if proc not in existing_processes_dict]

    if new_processes:
        cursor.execute("""
            CREATE TABLE #TempProcesos (
                IdProceso INT,
                NombreProceso NVARCHAR(255)
            )
        """)

        data_to_insert = [
            (len(existing_processes_dict) + i + 1, proc)
            for i, proc in enumerate(new_processes)
        ]

        cursor.executemany("""
            INSERT INTO #TempProcesos (IdProceso, NombreProceso)
            VALUES (?, ?)
        """, data_to_insert)

        cursor.execute("""
            INSERT INTO dbo.Procesos (IdProceso, NombreProceso)
            SELECT t.IdProceso, t.NombreProceso
            FROM #TempProcesos t
        """)

        cursor.execute("DROP TABLE #TempProcesos")

        conn.commit()

        for id_proc, proc in data_to_insert:
            existing_processes_dict[proc] = id_proc

    df['IdProceso'] = df['NombreProceso'].map(existing_processes_dict)
    
    return df

def update_groups(conn, df):
    """Update groups in the database and update the input DataFrame with group IDs.

    Args:
        conn (pyodbc.Connection): Database connection.
        df (pd.DataFrame): Input DataFrame containing 'NombreGrupo' column.

    Returns:
        pd.DataFrame: Updated DataFrame with 'IdGrupo' column.
    """
    print("Updating the Grupos table.")
    cursor = conn.cursor()
    cursor.execute('SELECT IdGrupo, NombreGrupo FROM dbo.Grupos')
    existing_groups = cursor.fetchall()
    existing_groups_dict = {row[1]: row[0] for row in existing_groups}
    nombres_grupo_unicos = df['NombreGrupo'].unique()
    new_groups = [grp for grp in nombres_grupo_unicos if grp not in existing_groups_dict]

    if new_groups:
        for grp in new_groups:
            new_id = len(existing_groups_dict) + 1
            cursor.execute('INSERT INTO dbo.Grupos (IdGrupo, NombreGrupo) VALUES (?, ?)',
                           (new_id, grp))
            existing_groups_dict[grp] = new_id
        conn.commit()

    df['IdGrupo'] = df['NombreGrupo'].map(existing_groups_dict)
    return df

def update_procesos_grupos(conn, df):
    """Update the ProcesosGrupos join table with unique IdProceso and IdGrupo pairs.

    Args:
        conn (pyodbc.Connection): Database connection.
        df (pd.DataFrame): Input DataFrame containing 'IdProceso' and 'IdGrupo' columns.

    Returns:
        None
    """
    print("Updating the ProcesosGrupos table.")
    cursor = conn.cursor()
    df = df.drop_duplicates(subset=['IdProceso', 'IdGrupo'])

    data_to_insert = [
        (row['IdProceso'], row['IdGrupo'])
        for _, row in df.iterrows()
    ]

    cursor.execute("""
        CREATE TABLE #TempProcesosGrupos (
            IdProceso INT,
            IdGrupo INT
        )
    """)

    cursor.executemany("""
        INSERT INTO #TempProcesosGrupos (IdProceso, IdGrupo)
        VALUES (?, ?)
    """, data_to_insert)

    cursor.execute("""
        INSERT INTO dbo.ProcesosGrupos (IdProcesoGrupo, IdProceso, IdGrupo)
        SELECT NEXT VALUE FOR proceso_grupo_seq, t.IdProceso, t.IdGrupo
        FROM #TempProcesosGrupos t
        LEFT JOIN dbo.ProcesosGrupos pg
        ON t.IdProceso = pg.IdProceso AND t.IdGrupo = pg.IdGrupo
        WHERE pg.IdProceso IS NULL AND pg.IdGrupo IS NULL
    """)

    cursor.execute("DROP TABLE #TempProcesosGrupos")
    conn.commit()

def update_fechas(conn, df):
    """
    Update the input DataFrame with date IDs from the Fechas table. 
    If the last date in the DataFrame is the first day of the month,
    insert the dates of the next month into the database with their respective IdFecha,
    the purpose of this is to help the forecasting model to predict the next month.

    Args:
        conn (pyodbc.Connection): Database connection.
        df (pd.DataFrame): Input DataFrame containing 'Fecha' column.

    Returns:
        pd.DataFrame: Updated DataFrame with 'IdFecha' column.
    """
    print("Updating the Fechas table.")
    cursor = conn.cursor()
    unique_dates = list(set(pd.to_datetime(df['Fecha']).dt.date))
    cursor.execute('SELECT COUNT(*) FROM dbo.Fechas')

    if cursor.fetchone()[0] == 0:
        unique_dates = sorted(unique_dates)
        last_date = unique_dates[-1]
        current_month_dates = pd.date_range(
            start=last_date + pd.Timedelta(days=1),
            end=last_date + pd.offsets.MonthEnd(0)
        )
        next_month_dates = pd.date_range(
            start=last_date + pd.offsets.MonthBegin(1),
            end=(last_date + pd.offsets.MonthBegin(2)) - pd.Timedelta(days=1)
        )
        unique_dates.extend(current_month_dates)
        unique_dates.extend(next_month_dates)

        for idx, date in enumerate(unique_dates, start=1):
            cursor.execute(
                'INSERT INTO dbo.Fechas (IdFecha, Fecha) VALUES (?, ?)',
                (idx, date)
            )
        conn.commit()
        cursor.execute('SELECT IdFecha, Fecha FROM dbo.Fechas')
        existing_dates = cursor.fetchall()
        existing_dates_dict = {row[1]: row[0] for row in existing_dates}
        df['IdFecha'] = pd.to_datetime(df['Fecha']).dt.date.map(existing_dates_dict)
    else:
        cursor.execute('SELECT IdFecha, Fecha FROM dbo.Fechas')
        existing_dates = cursor.fetchall()
        existing_dates_dict = {row[1]: row[0] for row in existing_dates}
        df['IdFecha'] = pd.to_datetime(df['Fecha']).dt.date.map(existing_dates_dict)

        if any(date.day == 1 for date in unique_dates):
            print("Inserting the next month dates into the Fechas table.")
            cursor.execute('SELECT MAX(Fecha) FROM dbo.Fechas;')
            last_db_date = cursor.fetchone()[0]

            if last_db_date:
                last_db_date = pd.to_datetime(last_db_date)
                next_month = last_db_date + pd.offsets.MonthBegin(1)
                next_month_dates = pd.date_range(
                    start=next_month,
                    end=(next_month + pd.offsets.MonthBegin(1)) - pd.Timedelta(days=1)
                )
                cursor.execute('SELECT MAX(IdFecha) FROM dbo.Fechas;')
                last_id_fecha = cursor.fetchone()[0] or 0
                new_id_fecha = last_id_fecha + 1

                for date in next_month_dates:
                    cursor.execute(
                        'INSERT INTO dbo.Fechas (IdFecha, Fecha) VALUES (?, ?)',
                        (new_id_fecha, date)
                    )
                    new_id_fecha += 1
                conn.commit()

    return df

def add_day_of_week_id(df):
    """
    Add a column 'IdDiaSemana' to the DataFrame based on the day of the week.

    Args:
        df (pd.DataFrame): Input DataFrame containing 'Fecha' column.

    Returns:
        pd.DataFrame: Updated DataFrame with 'IdDiaSemana' column.
    """
    mapeo_dias = {
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6,
        'Sunday': 7
    }
    df['DiaSemana'] = df['Fecha'].dt.day_name()
    df['IdDiaSemana'] = df['DiaSemana'].map(mapeo_dias)
    df = df.drop(columns=['DiaSemana'])

    return df

def filter_existing_rows(df, conn):
    """
    Filters out rows from the DataFrame that already exist in the database.
    In case the data does not exist in the database, the function returns the input DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    conn (pyodbc.Connection): The database connection.

    Returns:
    pd.DataFrame: The DataFrame with rows not existing in the database.
    """
    print("Identifying if the data already exists.")
    rows_to_remove = []
    cursor = conn.cursor()

    unique_id_fecha_df = list(set(df['IdFecha'].astype(int).tolist()))
    placeholders = ', '.join('?' for _ in unique_id_fecha_df)
    query = f'SELECT TOP 1 1 FROM dbo.ConsumosMIPS WHERE IdFecha IN ({placeholders})'
    cursor.execute(query, *unique_id_fecha_df)

    if cursor.fetchone() is None:
        print(
            "The new data from the dataset is able to be inserted."
        )
        return df

    print("The data already exists in the database. Filtering out the existing data.")
    for index, row in df.iterrows():
        query = """
        SELECT 1 FROM dbo.ConsumosMIPS
        WHERE IdProceso = ? AND IdGrupo = ? AND IdFecha = ? AND IdDiaSemana = ?
        """
        cursor.execute(
            query,
            (int(row['IdProceso']),
             int(row['IdGrupo']),
             int(row['IdFecha']),
             int(row['IdDiaSemana']))
        )
        result = cursor.fetchone()

        if result:
            rows_to_remove.append(index)

    df.drop(rows_to_remove, inplace=True)

    if len(rows_to_remove) > 0:
        print(
            f"{len(rows_to_remove)} row(s) you are trying to insert in the database already exist. "
            "No duplicated keys admitted. They won't be inserted."
        )
    elif len(rows_to_remove) == len(df):
        print("All data already exists in the database. Please provide a different dataset.")

    return df

def label_atypical_consumptions(conn, df):
    """
    Label atypical consumptions in the DataFrame and update the database.

    Args:
        conn (pyodbc.Connection): Database connection.
        df (pd.DataFrame): Input DataFrame containing 'total_mipsFecha' column.

    Returns:
        pd.DataFrame: Updated DataFrame with 'IdConsumo',
        'IdProceso',
        'IdGrupo',
        'IdFecha',
        'IdDiaSemana',
        'IdAtipico',
        and 'ConsumoMIPS' columns.
    """
    if df.empty:
        return df

    cursor = conn.cursor()
    df = df.rename(columns={'total_mipsFecha': 'ConsumoMIPS'})
    df['IdAtipico'] = 0
    cursor.execute('SELECT MAX(IdConsumo) FROM dbo.ConsumosMIPS')
    last_id = cursor.fetchone()[0] or 0
    next_id = last_id + 1

    print("Labeling atypical consumptions.")
    for (id_proceso, id_dia_semana), group in df.groupby(['IdProceso', 'IdDiaSemana']):
        id_proceso = int(id_proceso)
        id_dia_semana = int(id_dia_semana)

        if last_id == 0:
            stored_consumptions = group['ConsumoMIPS'].tolist()
        else:
            cursor.execute("""
                SELECT ConsumoMIPS FROM dbo.ConsumosMIPS
                WHERE IdProceso = ? AND IdDiaSemana = ? AND IdAtipico = 0
            """, (id_proceso, id_dia_semana))
            stored_consumptions = [row[0] for row in cursor.fetchall()]

        if stored_consumptions:
            q1 = np.percentile(stored_consumptions, 25)
            q3 = np.percentile(stored_consumptions, 75)
            iqr = q3 - q1
            max_val = q3 + 2.5 * iqr
            min_val = q1 - 2.5 * iqr

            conditions = [
                (group['ConsumoMIPS'] < min_val),
                (group['ConsumoMIPS'] > max_val)
            ]
            choices = [-1, 1]
            group['IdAtipico'] = np.select(conditions, choices, default=0)
            df.loc[group.index, 'IdAtipico'] = group['IdAtipico']

    df['IdConsumo'] = range(next_id, next_id + len(df))
    df = df[
        [
        'IdConsumo', 'IdProceso', 'IdGrupo', 'IdFecha', 
        'IdDiaSemana', 'IdAtipico', 'ConsumoMIPS'
        ]
    ]

    print("Updating the ConsumosMIPS table.")
    insert_query = """
        INSERT INTO dbo.ConsumosMIPS (IdConsumo, IdProceso, IdGrupo, IdFecha, IdDiaSemana, IdAtipico, ConsumoMIPS)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    data_to_insert = [
        (
            int(row['IdConsumo']),
            int(row['IdProceso']),
            int(row['IdGrupo']),
            int(row['IdFecha']),
            int(row['IdDiaSemana']),
            int(row['IdAtipico']),
            float(row['ConsumoMIPS'])
        )
        for _, row in df.iterrows()
    ]
    
    start_time = time.time()
    for i in range(0, len(data_to_insert), 1000):
        cursor.executemany(insert_query, data_to_insert[i:i+1000])
        conn.commit()
        if time.time() - start_time > 600:
            print("Process is still running...")
            start_time = time.time()

    return df
"""DETECTOR-DE-NOVEDADES/main_functions/novelty_detector.py"""
import pandas as pd
import numpy as np
import scipy.stats as stats

def segment_data(df):
    """
    Segments the input DataFrame into predefined date ranges.
    Args:
        df (pd.DataFrame): Input DataFrame containing a 'Fecha' column with date values.
    Returns:
        list of pd.DataFrame: A list of DataFrames, each corresponding
          to a segment of the input DataFrame
                              within the specified date ranges.
    """

    df.loc[:, 'Fecha'] = pd.to_datetime(df['Fecha'])
    segments = []
    date_ranges = [
        ('2021-01-01', '2022-05-29'),
        ('2022-05-29', '2023-04-03'),
        ('2023-04-03', '2023-07-01'),
        ('2023-07-01', '2024-11-01')
    ]

    for start_date, end_date in date_ranges:
        segment = df[(df['Fecha'] >= start_date) & (df['Fecha'] < end_date)]
        segments.append(segment)

    return segments

def label_atypical_values(new_consumptions, method='MAD', stored_consumptions=None):
    """
    Labels atypical values based on the 'ConsumoMIPS' column of the new_consumptions DataFrame.
    Parameters:
    new_consumptions (pd.DataFrame): DataFrame containing the new consumption data with a 
    'ConsumoMIPS' column.
    method (str, optional): Method to use for detecting atypical values. Options are 'MAD' 
    (Median Absolute Deviation) and 'IQR' (Interquartile Range). Default is 'MAD'.
    stored_consumptions (array-like, optional): Array-like object containing stored 
    consumption values to use for calculating the median and MAD or IQR. If None, calculations are based 
    on new_consumptions. Default is None.
    Returns:
    pd.DataFrame: The input DataFrame with an additional column 'IdAtipico' where -1 indicates
    a low atypical value, 1 indicates a high atypical value, and 0 indicates a typical value.
    """
    
    if method == 'MAD':
        if stored_consumptions is None:
            m = new_consumptions['ConsumoMIPS'].median()
            mad = stats.median_abs_deviation(new_consumptions['ConsumoMIPS'])
        else:
            m = np.median(stored_consumptions)
            mad = stats.median_abs_deviation(stored_consumptions)

        def label_value(x):
            if x < m - 3 * mad:
                return -1
            elif x > m + 3 * mad:
                return 1
            else:
                return 0
            
    elif method == 'MADadj':
        if stored_consumptions is None:
            m = new_consumptions['ConsumoMIPS'].median()
            mad = stats.median_abs_deviation(new_consumptions['ConsumoMIPS'])
        else:
            m = np.median(stored_consumptions)
            mad = stats.median_abs_deviation(stored_consumptions)

        def label_value(x):
            epsilon = 1
            if x < m - 3 * (mad + epsilon):
                return -1
            elif x > m + 3 * (mad + epsilon):
                return 1
            else:
                return 0

    elif method == 'IQR':
        if stored_consumptions is None:
            q1 = new_consumptions['ConsumoMIPS'].quantile(0.25)
            q3 = new_consumptions['ConsumoMIPS'].quantile(0.75)
        else:
            q1 = np.quantile(stored_consumptions, 0.25)
            q3 = np.quantile(stored_consumptions, 0.75)
        
        iqr = q3 - q1
        lower_bound = q1 - 3 * iqr
        upper_bound = q3 + 3 * iqr
        
        def label_value(x):
            if x < lower_bound:
                return -1
            elif x > upper_bound:
                return 1
            else:
                return 0

    new_consumptions.loc[:, 'IdAtipico'] = new_consumptions['ConsumoMIPS'].apply(label_value).values
    return new_consumptions

def detect_atypical_values(conn_insert, df: pd.DataFrame):
    """Detects atypical values in the given DataFrame and inserts the processed data into the database.
    Parameters:
    conn_insert (pyodbc.Connection): The database connection object used for inserting data.
    df (pd.DataFrame): The input DataFrame containing the data to be processed.
    Returns:
    str: A message indicating the result of the operation.
    Notes:
    - The function renames specific columns in the DataFrame for consistency.
    - It assigns unique IDs to each row in the DataFrame.
    - If the DataFrame is empty, it returns the DataFrame as is.
    - The function processes the data in segments and labels atypical values using different methods (MAD, IQR) based on the data characteristics.
    - The processed data is inserted into the database in batches to optimize performance.
    - The function handles both initial data insertion and updates to existing data.
    - It prints progress messages to indicate the status of the operation.
    """
    if df.empty:
        return df

    df = df.rename(columns={'total_mipsFecha': 'ConsumoMIPS', 'total_ejecucionesFecha': 'Ejecuciones'})
    df = df.sort_values(by=['Fecha', 'IdProceso'], ascending=[True, True])
    df['IdAtipico'] = 0
    t = 0
    m = 0
    ma = 0
    n = 0
    cursor = conn_insert.cursor()
    cursor.execute('SELECT MAX(IdConsumo) FROM dbo.ConsumosMIPS')
    last_id = cursor.fetchone()[0] or 0
    next_id = last_id + 1

    df['IdConsumo'] = range(next_id, next_id + len(df))

    df_to_insert = pd.DataFrame()

    def insert_data(df_to_insert):
        cursor.fast_executemany = True
        
        insert_query = """
            INSERT INTO dbo.ConsumosMIPS (IdConsumo, IdProceso, IdGrupo, IdFecha, IdDiaSemana, IdAtipico, Ejecuciones, ConsumoMIPS)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        data_to_insert = df_to_insert.astype({
            'IdConsumo': 'int',
            'IdProceso': 'int',
            'IdGrupo': 'int',
            'IdFecha': 'int',
            'IdDiaSemana': 'int',
            'IdAtipico': 'int',
            'Ejecuciones': 'int',
            'ConsumoMIPS': 'float'
        }).to_records(index=False)

        cursor.executemany(insert_query, data_to_insert.tolist())
        conn_insert.commit()

    if last_id == 0:
        df = df[['IdConsumo', 'IdProceso', 'IdGrupo', 'IdFecha', 'IdDiaSemana', 'IdAtipico', 'Ejecuciones', 'ConsumoMIPS', 'Fecha']]
        count_df = df['IdProceso'].value_counts().reset_index()
        count_df.columns = ['IdProceso', 'Count']
        df_idprocess_one_execution = count_df[count_df['Count'] == 1]

        df_one_execution = df[df['IdProceso'].isin(df_idprocess_one_execution['IdProceso'])]
        df_one_execution_labeled = label_atypical_values(df_one_execution, method='MAD')
        df_to_insert = pd.concat([df_to_insert, df_one_execution_labeled])

        df_more_than_one_execution = df[~df['IdProceso'].isin(df_idprocess_one_execution['IdProceso'])]
        segments = segment_data(df_more_than_one_execution)

        for i, segment in enumerate(segments):
            print("Detecting atypical values...")
            for id_process in segment['IdProceso'].unique():
                process_data = segment[segment['IdProceso'] == id_process]
                if process_data['IdGrupo'].nunique() > 1:
                    daily_segments = []
                    segments_by_group = [process_data[process_data['IdGrupo'] == group] for group in process_data['IdGrupo'].unique()]
                    for segment_by_group in segments_by_group:
                        daily_segments_by_group = [segment_by_group[segment_by_group['IdDiaSemana'] == day] for day in segment_by_group['IdDiaSemana'].unique()]
                        daily_segments.extend(daily_segments_by_group)
                else:
                    daily_segments = [process_data[process_data['IdDiaSemana'] == day] for day in process_data['IdDiaSemana'].unique()]
                for daily_segment in daily_segments:
                    if len(daily_segment) == 1:
                        daily_segment = label_atypical_values(daily_segment, method='MAD', stored_consumptions=process_data['ConsumoMIPS'].tolist())
                        m += 1
                    elif len(daily_segment) < 20:
                        daily_segment = label_atypical_values(daily_segment, method='MAD')
                        m += 1
                    elif len(daily_segment) >= 20:
                        data_normal_test = daily_segment['ConsumoMIPS'].tolist()
                        if np.ptp(data_normal_test) == 0:
                            daily_segment = label_atypical_values(daily_segment, method='MADadj')
                            ma += 1
                        else:
                            normal_test = stats.shapiro(daily_segment['ConsumoMIPS'].tolist())[1] > 0.05
                            if normal_test:
                                daily_segment = label_atypical_values(daily_segment, method='IQR')
                                n += 1
                            else:
                                daily_segment = label_atypical_values(daily_segment, method='MAD')
                                m += 1
                    if not daily_segment.empty:
                        df_to_insert = pd.concat([df_to_insert, daily_segment], ignore_index=True)
        
            print("Updating the ConsumosMIPS table.")
            df_to_insert = df_to_insert[['IdConsumo', 'IdProceso', 'IdGrupo', 'IdFecha', 'IdDiaSemana', 'IdAtipico', 'Ejecuciones', 'ConsumoMIPS']]
            insert_data(df_to_insert)
            print(f"Segement number {i+1} loaded")
            df_to_insert = pd.DataFrame()

    else:
        df = df[['IdConsumo', 'IdProceso', 'IdGrupo', 'IdFecha', 'IdDiaSemana', 'IdAtipico', 'Ejecuciones', 'ConsumoMIPS']]
        
        cursor.execute("SELECT IdFecha FROM dbo.Fechas WHERE Fecha = '2023-07-01';")
        start_id_fecha = cursor.fetchone()[0]
        for id_fecha in sorted(df['IdFecha'].unique()):
            print("Detecting atypical values...")
            data_fecha = df[df['IdFecha'] == id_fecha]
            for _, row in data_fecha.iterrows():
                id_process = row['IdProceso']
                id_group = row['IdGrupo']
                id_diasemana = row['IdDiaSemana']
                new_consumption = data_fecha[(data_fecha['IdProceso'] == id_process) & (data_fecha['IdGrupo'] == id_group) & (data_fecha['IdDiaSemana'] == id_diasemana)]
                cursor.execute(f"""SELECT ConsumoMIPS FROM dbo.ConsumosMIPS WHERE IdProceso = {id_process} AND IdGrupo = {id_group} AND IdDiaSemana = {id_diasemana} AND IdFecha >= {start_id_fecha};""")
                stored_consumptions = [row[0] for row in cursor.fetchall()]
                if len(stored_consumptions) == 0:
                    new_consumption.loc[:, 'IdAtipico'] = 1
                    t += 1
                elif len(stored_consumptions) == 1:
                    t += 1
                    if (new_consumption['ConsumoMIPS'].values[0] - stored_consumptions[0]) > 3:
                        new_consumption.loc[:, 'IdAtipico'] = 1
                    elif (new_consumption['ConsumoMIPS'].values[0] - stored_consumptions[0]) < -3:
                        new_consumption.loc[:, 'IdAtipico'] = -1
                    else:
                        new_consumption.loc[:, 'IdAtipico'] = 0
                elif len(stored_consumptions) < 20:
                    new_consumption = label_atypical_values(new_consumption, method='MAD', stored_consumptions=stored_consumptions)
                    m += 1
                elif len(stored_consumptions) >= 20:
                    if np.ptp(stored_consumptions) == 0:
                        new_consumption = label_atypical_values(new_consumption, method='MADadj', stored_consumptions=stored_consumptions)
                        ma += 1
                    else:
                        normal_test = stats.shapiro(stored_consumptions)[1] > 0.05
                        if normal_test:
                            new_consumption = label_atypical_values(new_consumption, method='IQR', stored_consumptions=stored_consumptions)
                            n += 1
                        else:
                            new_consumption = label_atypical_values(new_consumption, method='MAD', stored_consumptions=stored_consumptions)
                            m += 1
                
                df_to_insert = pd.concat([df_to_insert, new_consumption], ignore_index=True)
            
            print("Updating the ConsumosMIPS table.")
            insert_data(df_to_insert)
            df_to_insert = pd.DataFrame()

    return f'Data updated successfully. {t + m + ma + n} processes were labeled. {m} using the MAD method, {ma} using the MAD Adjusted, and {n} processes were labeled using the IQR method.'

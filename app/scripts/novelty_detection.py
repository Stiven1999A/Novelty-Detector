import time
import pandas as pd
import numpy as np
import scipy.stats as stats

def segment_data(df):
    #df['Fecha'] = pd.to_datetime(df['Fecha'])
    segment1 = df[df['Fecha'] <= '2021-01-31']
    segment2 = df[(df['Fecha'] > '2021-01-31') & (df['Fecha'] <= '2021-02-28')]
    segment3 = df[(df['Fecha'] > '2021-02-28') & (df['Fecha'] <= '2021-03-20')]
    segment4 = df[(df['Fecha'] > '2021-03-20') & (df['Fecha'] <= '2021-03-31')]
    return [segment1, segment2, segment3, segment4]

def label_atypical_values(new_consumptions, method='MAD', stored_consumptions=None):
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

    new_consumptions['IdAtipico'] = 0  # Initialize the column with default values
    new_consumptions['IdAtipico'].iloc[:] = new_consumptions['ConsumoMIPS'].apply(label_value).values
    return new_consumptions

def detect_atypical_values(conn_insert, df: pd.DataFrame):

    if df.empty:
        return df

    df = df.rename(columns={'total_mipsFecha': 'ConsumoMIPS', 'total_ejecucionesFecha': 'Ejecuciones'})
    df['IdAtipico'] = 0

    cursor = conn_insert.cursor()
    cursor.execute('SELECT MAX(IdConsumo) FROM dbo.ConsumosMIPS')
    last_id = cursor.fetchone()[0] or 0
    next_id = last_id + 1

    df['IdConsumo'] = range(next_id, next_id + len(df))

    df_to_insert = pd.DataFrame()

    if last_id == 0:

        df = df[
            ['IdConsumo', 'IdProceso', 'IdGrupo', 'IdFecha', 'IdDiaSemana',
            'IdAtipico', 'Ejecuciones', 'ConsumoMIPS', 'Fecha']
            ]

        count_df = df['IdProceso'].value_counts().reset_index()
        count_df.columns = ['IdProceso', 'Count']
        df_idprocess_one_execution = count_df[count_df['Count'] == 1]

        df_one_execution = df[df['IdProceso'].isin(df_idprocess_one_execution['IdProceso'])]
        df_one_execution_labeled = label_atypical_values(df_one_execution, method='MAD')
        df_to_insert = pd.concat([df_to_insert, df_one_execution_labeled])

        df_more_than_one_execution = df[~df['IdProceso'].isin(df_idprocess_one_execution['IdProceso'])]
        segments = segment_data(df_more_than_one_execution)

        for segment in segments:
            for id_process in segment['IdProceso'].unique():
                process_data = segment[segment['IdProceso'] == id_process]
                daily_segments = [process_data[process_data['IdDiaSemana'] == day] for day in process_data['IdDiaSemana'].unique()]
                for daily_segment in daily_segments:
                    if daily_segment.empty:
                        continue
                    elif len(daily_segment) == 1:
                        daily_segment = label_atypical_values(daily_segment, method='MAD', stored_consumptions=process_data['ConsumoMIPS'].tolist())
                    elif len(daily_segment) < 20:
                        daily_segment = label_atypical_values(daily_segment, method='MAD')
                    else:
                        normal_test = stats.shapiro(daily_segment['ConsumoMIPS'])[1] > 0.05
                        if normal_test:
                            daily_segment = label_atypical_values(daily_segment, method='IQR')
                        else:
                            daily_segment = label_atypical_values(daily_segment, method='MAD')

                    df_to_insert = pd.concat([df_to_insert, daily_segment], ignore_index=True)
        print("Updating the ConsumosMIPS table.")
        insert_query = """
            INSERT INTO dbo.ConsumosMIPS (IdConsumo, IdProceso, IdGrupo, IdFecha, IdDiaSemana, IdAtipico, Ejecuciones, ConsumoMIPS)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        data_to_insert = [
            (
                int(row['IdConsumo']),
                int(row['IdProceso']),
                int(row['IdGrupo']),
                int(row['IdFecha']),
                int(row['IdDiaSemana']),
                int(row['IdAtipico']),
                float(row['Ejecuciones']),
                float(row['ConsumoMIPS'])
            )
            for _, row in df_to_insert.iterrows()
        ]
        
        start_time = time.time()
        for i in range(0, len(data_to_insert), 1000):
            cursor.executemany(insert_query, data_to_insert[i:i+1000])
            conn_insert.commit()
            if time.time() - start_time > 600:
                print("Process is still running...")
                start_time = time.time()

    else:

        df = df[
            ['IdConsumo', 'IdProceso', 'IdGrupo', 'IdFecha', 'IdDiaSemana',
            'IdAtipico', 'Ejecuciones', 'ConsumoMIPS']
            ]
        
        cursor.execute("SELECT IdFecha FROM dbo.Fechas WHERE Fecha = '2021-03-20'")
        start_id_fecha = cursor.fetchone()[0]

        cursor.execute("SELECT IdFecha FROM dbo.Fechas WHERE Fecha = '2021-03-31'")
        end_id_fecha = cursor.fetchone()[0]
        
        for id_fecha in sorted(df['IdFecha'].unique()):
            data_fecha = df[df['IdFecha'] == id_fecha]
            id_diasemana = data_fecha['IdDiaSemana'].unique()
            id_processes = sorted(data_fecha['IdProceso'].unique())
            for id_process in id_processes:
                new_consumption = data_fecha[data_fecha['IdProceso'] == id_process]
                if len(new_consumption) > 1:
                    new_consumption = new_consumption.iloc[0:1].copy()
                    new_consumption['ConsumoMIPS'] = data_fecha[data_fecha['IdProceso'] == id_process]['ConsumoMIPS'].sum()
                    new_consumption['Ejecuciones'] = data_fecha[data_fecha['IdProceso'] == id_process]['Ejecuciones'].sum()

                cursor.execute(f"""SELECT ConsumoMIPS FROM dbo.ConsumosMIPS WHERE IdProceso = {id_process} AND IdDiaSemana = {id_diasemana} AND IdFecha BETWEEN {start_id_fecha} AND {end_id_fecha};""")
                stored_consumptions = [row[0] for row in cursor.fetchall()]
        
                if len(stored_consumptions) == 0:
                    new_consumption['IdAtipico'] = 1
                
                elif len(stored_consumptions) == 1:
                    if (new_consumption['ConsumoMIPS'].values[0] - stored_consumptions[0]) > 1:
                        new_consumption['IdAtipico'] = 1
                    elif (new_consumption['ConsumoMIPS'].values[0] - stored_consumptions[0]) < -1:
                        new_consumption['IdAtipico'] = -1
                    else:
                        new_consumption['IdAtipico'] = 0
                elif len(stored_consumptions) < 20:
                    new_consumption = label_atypical_values(new_consumption, method='MAD', stored_consumptions=stored_consumptions)
                else:
                    normal_test = stats.shapiro(stored_consumptions)[1] > 0.05
                    if normal_test:
                        new_consumption = label_atypical_values(new_consumption, method='IQR', stored_consumptions=stored_consumptions)
                    else:
                        new_consumption = label_atypical_values(new_consumption, method='MAD', stored_consumptions=stored_consumptions)
                
                df_to_insert = pd.concat([df_to_insert, new_consumption], ignore_index=True)
            
            print("Updating the ConsumosMIPS table.")
            insert_query = """
                INSERT INTO dbo.ConsumosMIPS (IdConsumo, IdProceso, IdGrupo, IdFecha, IdDiaSemana, IdAtipico, Ejecuciones, ConsumoMIPS)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            data_to_insert = [
                (
                    int(row['IdConsumo']),
                    int(row['IdProceso']),
                    int(row['IdGrupo']),
                    int(row['IdFecha']),
                    int(row['IdDiaSemana']),
                    int(row['IdAtipico']),
                    float(row['Ejecuciones']),
                    float(row['ConsumoMIPS'])
                )
                for _, row in df_to_insert.iterrows()
            ]
            
            start_time = time.time()
            for i in range(0, len(data_to_insert), 1000):
                cursor.executemany(insert_query, data_to_insert[i:i+1000])
                conn_insert.commit()
                if time.time() - start_time > 600:
                    print("Process is still running...")
                    start_time = time.time()

            df_to_insert = pd.DataFrame()

    return 'Data updated successfully'

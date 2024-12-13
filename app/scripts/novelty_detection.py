import time
import pandas as pd
import numpy as np
import scipy.stats as stats
from app.database_tools.connections import connect_to_fetch_data, connect_to_insert_data

def segment_data(df):
    #df['Fecha'] = pd.to_datetime(df['Fecha'])
    segment1 = df[df['Fecha'] <= '2021-01-31']
    segment2 = df[(df['Fecha'] > '2021-01-31') & (df['Fecha'] <= '2021-02-28')]
    segment3 = df[(df['Fecha'] > '2021-02-28') & (df['Fecha'] <= '2021-03-20')]
    segment4 = df[(df['Fecha'] > '2021-03-20') & (df['Fecha'] <= '2021-03-31')]
    return [segment1, segment2, segment3, segment4]

def label_atypical_values_MAD(df):
    m = df['ConsumoMIPS'].median()
    mad = stats.median_abs_deviation(df['ConsumoMIPS'])
    def label_value(x):
        if x < m - 3 * mad:
            return -1
        elif x > m + 3 * mad:
            return 1
        else:
            return 0
    df['IdAtipico'] = df['ConsumoMIPS'].apply(label_value)
    return df

def novelty_detector_MAD(new_cunsuption: pd.DataFrame, stored_consumptions: list):
    m = np.median(stored_consumptions)
    mad = stats.median_abs_deviation(stored_consumptions)
    def label_value(x):
        if x < m - 3 * mad:
            return -1
        elif x > m + 3 * mad:
            return 1
        else:
            return 0
    new_cunsuption['IdAtipico'] = new_cunsuption['ConsumoMIPS'].apply(label_value)
    return new_cunsuption

def label_atypical_values_IQR(df):
    q1 = df['ConsumoMIPS'].quantile(0.25)
    q3 = df['ConsumoMIPS'].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 2.5 * iqr
    upper_bound = q3 + 2.5 * iqr
    def label_value(x):
        if x < lower_bound:
            return -1
        elif x > upper_bound:
            return 1
        else:
            return 0
    df['IdAtipico'] = df['ConsumoMIPS'].apply(label_value)
    return df

def novelty_detector_IQR(new_cunsuption: pd.DataFrame, stored_consumptions: list):
    q1 = np.quantile(stored_consumptions, 0.25)
    q3 = np.quantile(stored_consumptions, 0.75)
    iqr = q3 - q1
    lower_bound = q1 - 2.5 * iqr
    upper_bound = q3 + 2.5 * iqr
    def label_value(x):
        if x < lower_bound:
            return -1
        elif x > upper_bound:
            return 1
        else:
            return 0
    new_cunsuption['IdAtipico'] = new_cunsuption['ConsumoMIPS'].apply(label_value)
    return new_cunsuption

def atypical_labeling_orchestrator(df):

    weekdays = df[df['IdDiaSemana'] <= 5]
    weekends = df[df['IdDiaSemana'] > 5]

    if len(weekdays) > 20 and len(weekends) > 20:
        normal_weekdays = stats.shapiro(weekdays['ConsumoMIPS'])[1] > 0.05
        normal_weekends = stats.shapiro(weekends['ConsumoMIPS'])[1] > 0.05

        if normal_weekdays and normal_weekends:
            anova_result = stats.f_oneway(weekdays['ConsumoMIPS'], weekends['ConsumoMIPS'])[1] < 0.05
            if anova_result:
                weekdays = label_atypical_values_IQR(weekdays)
                weekends = label_atypical_values_IQR(weekends)
                return pd.concat([weekdays, weekends])
            else:
                return label_atypical_values_IQR(df)
        else:
            kruskal_result = stats.kruskal(weekdays['ConsumoMIPS'], weekends['ConsumoMIPS'])[1] < 0.05
            if kruskal_result:
                weekdays = label_atypical_values_MAD(weekdays)
                weekends = label_atypical_values_MAD(weekends)
                return pd.concat([weekdays, weekends])
            else:
                return label_atypical_values_MAD(df)
    else:
        if len(df) >= 20:
            normal_all = stats.shapiro(df['ConsumoMIPS'])[1] > 0.05
            if normal_all:
                return label_atypical_values_IQR(df)
            else:
                return label_atypical_values_MAD(df)
        else:
            return label_atypical_values_MAD(df)

def novelty_detector_orchestrator(stored_consumptions: pd.DataFrame, new_consumption: pd.DataFrame):

    weekdays = stored_consumptions[stored_consumptions['IdDiaSemana'] <= 5]
    weekends = stored_consumptions[stored_consumptions['IdDiaSemana'] > 5]

    if len(weekdays) > 20 and len(weekends) > 20:
        normal_weekdays = stats.shapiro(weekdays['ConsumoMIPS'])[1] > 0.05
        normal_weekends = stats.shapiro(weekends['ConsumoMIPS'])[1] > 0.05

        if normal_weekdays and normal_weekends:
            anova_result = stats.f_oneway(weekdays['ConsumoMIPS'], weekends['ConsumoMIPS'])[1] < 0.05
            if anova_result:
                if new_consumption['IdDiaSemana'][0] <= 5:
                    weekdays = novelty_detector_IQR(new_consumption, weekdays['ConsumoMIPS'].tolist())
                    return weekdays
                else:
                    weekends = novelty_detector_IQR(new_consumption, weekends['ConsumoMIPS'].tolist())
                    return weekends
            else:
                return novelty_detector_IQR(new_consumption, stored_consumptions['ConsumoMIPS'].tolist())
        else:
            kruskal_result = stats.kruskal(weekdays['ConsumoMIPS'], weekends['ConsumoMIPS'])[1] < 0.05
            if kruskal_result:
                if new_consumption['IdDiaSemana'][0] <= 5:
                    weekdays = novelty_detector_MAD(new_consumption, weekdays['ConsumoMIPS'].tolist())
                    return weekdays
                else:
                    weekends = novelty_detector_MAD(new_consumption, weekends['ConsumoMIPS'].tolist())
                    return weekends
            else:
                return novelty_detector_MAD(new_consumption, stored_consumptions['ConsumoMIPS'].tolist())
    else:
        if len(stored_consumptions) >= 20:
            normal_all = stats.shapiro(stored_consumptions['ConsumoMIPS'])[1] > 0.05
            if normal_all:
                return novelty_detector_IQR(new_consumption, stored_consumptions['ConsumoMIPS'].tolist())
            else:
                return novelty_detector_MAD(new_consumption, stored_consumptions['ConsumoMIPS'].tolist())
        else:
            return novelty_detector_MAD(new_consumption, stored_consumptions['ConsumoMIPS'].tolist())

def detect_atypical_values(conn_insert, conn_fetch, df: pd.DataFrame):

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
        df_one_execution_labeled = label_atypical_values_MAD(df_one_execution)
        df_one_execution_labeled = df_one_execution_labeled.drop(columns=['Fecha'])
        df_to_insert = pd.concat([df_to_insert, df_one_execution_labeled])

        df_more_than_one_execution = df[~df['IdProceso'].isin(df_idprocess_one_execution['IdProceso'])]
        segments = segment_data(df_more_than_one_execution)

        for segment in segments:

            for id_process in segment['IdProceso'].unique():

                process_data = segment[segment['IdProceso'] == id_process]
                process_data = atypical_labeling_orchestrator(process_data)
                process_data = process_data.drop(columns=['Fecha'])
                df_to_insert = pd.concat([df_to_insert, process_data])
        
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
        
        for id_fecha in sorted(df['IdFecha'].unique()):

            df_id_fecha = df[df['IdFecha'] == id_fecha]
            for id_process in sorted(df_id_fecha['IdProceso'].unique()):
                
                cursor.execute("SELECT IdFecha FROM dbo.Fechas WHERE Fecha = '2021-03-20'")
                start_id_fecha = cursor.fetchone()[0]

                cursor.execute("SELECT IdFecha FROM dbo.Fechas WHERE Fecha = '2021-03-31'")
                end_id_fecha = cursor.fetchone()[0]

                query = (f"""SELECT * FROM dbo.ConsumosMIPS WHERE IdProceso = {id_process} AND IdFecha BETWEEN {start_id_fecha} AND {end_id_fecha};""")
                stored_consumptions = pd.read_sql(query, conn_fetch)
        
                if len(stored_consumptions) == 0:
                    new_consumption = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)].copy()

                    if len(new_consumption) > 1:
                        new_consumption = new_consumption.iloc[0:1].copy()
                        new_consumption['ConsumoMIPS'] = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)]['ConsumoMIPS'].sum()
                        new_consumption['Ejecuciones'] = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)]['Ejecuciones'].sum()

                    new_consumption['IdAtipico'] = 1
                
                elif len(stored_consumptions) == 1:
                    new_consumption = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)].copy()

                    if len(new_consumption) > 1:
                        new_consumption = new_consumption.iloc[0:1].copy()
                        new_consumption['ConsumoMIPS'] = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)]['ConsumoMIPS'].sum()
                        new_consumption['Ejecuciones'] = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)]['Ejecuciones'].sum()

                    if (new_consumption['ConsumoMIPS'] - stored_consumptions['ConsumoMIPS'][0]) > 1:
                        new_consumption['IdAtipico'] = 1
                    elif (new_consumption['ConsumoMIPS'][0] - stored_consumptions['ConsumoMIPS'][0]) < -1:
                        new_consumption['IdAtipico'] = -1
                    else:
                        new_consumption['IdAtipico'] = 0
                
                elif len(stored_consumptions) > 1:
                    new_consumption = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)].copy()

                    if len(new_consumption) > 1:
                        new_consumption = new_consumption.iloc[0:1].copy()
                        new_consumption['ConsumoMIPS'] = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)]['ConsumoMIPS'].sum()
                        new_consumption['Ejecuciones'] = df[(df['IdFecha'] == id_fecha) & (df['IdProceso'] == id_process)]['Ejecuciones'].sum()
                    
                    new_consumption = novelty_detector_orchestrator(stored_consumptions, new_consumption)
                
                df_to_insert = pd.concat([df_to_insert, new_consumption])
            
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

def main():

    conn_insert = connect_to_insert_data()
    conn_fetch = connect_to_fetch_data()

    df = pd.read_csv('test-file.csv', decimal='.')
    
    print(df.head())

    print(detect_atypical_values(conn_insert, conn_fetch, df))

    conn_insert.close()
    conn_fetch.close()


if __name__ == "__main__":
    main()

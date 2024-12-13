import pandas as pd
import scipy.stats as stats
import scikit_posthocs as sp
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from database_tools.connections import connect_to_fetch_data

def fetch_new_data(conn_fetch):
    query = "SELECT * FROM dbo.refrescarprocesos_10dias"
    df = pd.read_sql(query, conn_fetch)
    return df

def detect_atypical_values(df):
    # Group by 'NombreProceso' and sum 'total_mipsFecha' by day
    grouped = df.groupby(['NombreProceso', df['Fecha'].dt.date])['total_mipsFecha'].sum().reset_index()
    
    # Function to detect atypical values using MAD
    def mad_based_outlier(points, threshold=3.5):
        if len(points) == 0:
            return pd.Series([False] * len(points))
        median = points.median()
        diff = (points - median).abs()
        mad = diff.median()
        modified_z_score = 0.6745 * diff / mad
        return modified_z_score > threshold
    
    # Apply the MAD method to each group
    grouped['Atypical'] = grouped.groupby('NombreProceso')['total_mipsFecha'].transform(mad_based_outlier)
    
    return grouped

def main():
    conn_fetch = connect_to_fetch_data()
    df = fetch_new_data(conn_fetch)
    
    atypical_df = detect_atypical_values(df)
    print(atypical_df)
    
    # Save the DataFrame to a CSV file
    atypical_df.to_csv('atypical_values.csv', index=False, decimal=',')
    print("Atypical values saved to 'atypical_values.csv'")

if __name__ == "__main__":
    main()
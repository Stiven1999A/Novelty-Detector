import pandas as pd
import scipy.stats as stats
import scikit_posthocs as sp
import matplotlib.pyplot as plt
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from app.database_tools.connections import connect_to_fetch_data

def fetch_new_data(conn_fetch):
    query = "SELECT * FROM dbo.refrescarprocesos_10dias"
    df = pd.read_sql(query, conn_fetch)
    return df

def segment_data(df):
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    
    segment1 = df[df['Fecha'] < '2022-05-29']
    segment2 = df[(df['Fecha'] >= '2022-05-29') & (df['Fecha'] < '2023-04-03')]
    segment3 = df[(df['Fecha'] >= '2023-04-03') & (df['Fecha'] < '2023-07-01')]
    segment4 = df[df['Fecha'] >= '2023-07-01']
    
    return segment1, segment2, segment3, segment4

def sum_by_day(segment):
    return segment.groupby(segment['Fecha'].dt.date)['total_mipsFecha'].sum()

def perform_normality_test(segment1, segment2, segment3, segment4):
    segments = [segment1, segment2, segment3, segment4]
    normal = True
    for i, segment in enumerate(segments, start=1):
        stat, p = stats.shapiro(segment)
        print(f"Segment {i} - Shapiro-Wilk Test: Statistics={stat}, p-value={p}")
        if p > 0.05:
            print(f"Segment {i} looks Gaussian (fail to reject H0)")
        else:
            print(f"Segment {i} does not look Gaussian (reject H0)")
            normal = False
    return normal

def perform_anova_and_tukey(segment1, segment2, segment3, segment4):
    data = pd.DataFrame({
        'Segment': ['Segment1'] * len(segment1) + ['Segment2'] * len(segment2) + ['Segment3'] * len(segment3) + ['Segment4'] * len(segment4),
        'Sum': pd.concat([segment1, segment2, segment3, segment4])
    })
    
    anova_result = stats.f_oneway(segment1, segment2, segment3, segment4)
    print(f"ANOVA result: {anova_result}")
    
    tukey_result = pairwise_tukeyhsd(data['Sum'], data['Segment'])
    print(tukey_result)

def perform_kruskal_wallis(segment1, segment2, segment3, segment4):
    kruskal_result = stats.kruskal(segment1, segment2, segment3, segment4)
    print(f"Kruskal-Wallis result: {kruskal_result}")
    
    data = pd.DataFrame({
        'Segment': ['Segment1'] * len(segment1) + ['Segment2'] * len(segment2) + ['Segment3'] * len(segment3) + ['Segment4'] * len(segment4),
        'Sum': pd.concat([segment1, segment2, segment3, segment4])
    })
    
    dunn_result = sp.posthoc_dunn(data, val_col='Sum', group_col='Segment', p_adjust='bonferroni')
    print(dunn_result)

def main():
    conn_fetch = connect_to_fetch_data()
    df = fetch_new_data(conn_fetch)

    segment1, segment2, segment3, segment4 = segment_data(df)
    
    segment1_sum = sum_by_day(segment1)
    segment2_sum = sum_by_day(segment2)
    segment3_sum = sum_by_day(segment3)
    segment4_sum = sum_by_day(segment4)
    
    normal = perform_normality_test(segment1_sum, segment2_sum, segment3_sum, segment4_sum)
    
    if normal:
        perform_anova_and_tukey(segment1_sum, segment2_sum, segment3_sum, segment4_sum)
    else:
        perform_kruskal_wallis(segment1_sum, segment2_sum, segment3_sum, segment4_sum)

if __name__ == "__main__":
    main()
    
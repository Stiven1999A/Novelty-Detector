import pandas as pd
import scipy.stats as stats
import scikit_posthocs as sp
import matplotlib.pyplot as plt
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from app.database_tools.connections import connect_to_fetch_data

conn_fetch = connect_to_fetch_data()

query = "SELECT * FROM dbo.refrescarprocesos_10dias"

df = pd.read_sql(query, conn_fetch)
print(df.head())

unique_processes = df['NombreProceso'].nunique()
print(f'There are {unique_processes} unique processes in the dataset')

mean_total_mipsFecha_per_day = df.groupby('Fecha')['total_mipsFecha'].mean().reset_index()
mean_total_mipsFecha_per_day.columns = ['Fecha', 'Mean_total_mipsFecha']
stat, p_value = stats.shapiro(mean_total_mipsFecha_per_day['Mean_total_mipsFecha'])
print(f'Shapiro-Wilk test statistic: {stat}, p-value: {p_value}')
mean_total_mipsFecha_per_day.to_csv('//home//mips-sinco-estadisticas//app//exploratory_analysis//mean_total_mipsFecha_per_day.csv', index=False, decimal=',')
print(mean_total_mipsFecha_per_day)

category_counts = df['NombreProceso'].value_counts().reset_index()
print(category_counts)
category_counts.columns = ['NombreProceso', 'Count']

df = df.merge(category_counts, on='NombreProceso', how='left')
df_analysis = df[['Count', 'total_mipsFecha']]
filtered_df = df_analysis[df_analysis['Count'] <= 29]

plt.figure(figsize=(12, 8))
plt.boxplot([filtered_df[filtered_df['Count'] == count]['total_mipsFecha'] for count in filtered_df['Count'].unique()],
            labels=filtered_df['Count'].unique())
plt.xlabel('Count')
plt.ylabel('total_mipsFecha')
plt.title('Boxplot of total_mipsFecha for Counts less than 29')
plt.grid(True)
plt.show()

#df_analysis.to_csv('//home//mips-sinco-estadisticas//app//exploratory_analysis//distribution_mips_by_count.csv', index=False, decimal=',')
print(category_counts)

normality_results = []

for count in filtered_df['Count'].unique():
    subset = filtered_df[filtered_df['Count'] == count]['total_mipsFecha']
    stat, p_value = stats.shapiro(subset)
    normality_results.append({'Count': count, 'p-value': p_value})

normality_df = pd.DataFrame(normality_results)
print(normality_df)

count_proportions = category_counts['Count'].value_counts(normalize=True).reset_index()
count_proportions.columns = ['Count', 'Proportion']
    
count_proportions.to_csv('//home//mips-sinco-estadisticas//app//exploratory_analysis//proportions.csv', index=False, decimal=',')

plt.figure(figsize=(10, 8))
plt.barh(count_proportions['Count'], count_proportions['Proportion'], color='skyblue')
plt.xlabel('Proportion')
plt.ylabel('Count')
plt.title('Proportion of Counts')
plt.grid(axis='x')
    
print(count_proportions)

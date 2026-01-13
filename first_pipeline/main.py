import sys
import pandas as pd

month = int(sys.argv[1])

df = pd.DataFrame({'A': [1], 'B': [3]})
df.loc[1] = [2, 4]
df.loc[2] = [5, 6]
df['C'] = df['A'] + df['B']
df['month'] = month

print(df.head())

df.to_csv(f'first_pipeline_output_month_{month}.csv', index=False)
df.to_parquet(f'first_pipeline_output_month_{month}.parquet', index=False)




print('arguments', sys.argv[2])
print("First pipeline executed, month:", month)
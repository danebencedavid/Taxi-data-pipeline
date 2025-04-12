import pandas as pd

df = pd.read_parquet('yellow_tripdata_2024-12.parquet').to_csv('yellow_tripdata_2024-12.csv')

df_sample = pd.read_csv('yellow_tripdata_2024-12.csv',nrows=10000)

df_sample.to_csv('sample_2024-12.csv',index=False)




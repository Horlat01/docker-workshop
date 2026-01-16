#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine



dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():

    host = 'localhost'
    port = 5432
    postgres_db = 'ny_taxi'
    postgres_user = 'root'
    postgres_password = 'root'
    
    
    year = 2020
    month = 1

    chunksize = 100000
    target_table = 'yellow_taxi_data'

    engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{host}:{port}/{postgres_db}')
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator= True,
        chunksize = chunksize
    )


    First = True
    for df_chunk in tqdm(df_iter):
        if First:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            First = False
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')


if __name__ == '__main__':
    run()





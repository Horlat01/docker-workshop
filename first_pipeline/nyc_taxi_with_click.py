#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
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


@click.command()
@click.option('--host', default='pgdatabase', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--year', default=2020, type=int, help='Year of taxi data')
@click.option('--month', default=1, type=int, help='Month of taxi data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
def run(host, port, db, user, password, year, month, chunksize, table):
    """Download and load NYC taxi data into PostgreSQL."""
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    click.echo(f"Loading data from: {url}")
    
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=table, con=engine, if_exists='replace')
            first = False
        df_chunk.to_sql(name=table, con=engine, if_exists='append')
    
    click.echo("Data loading completed successfully!")


if __name__ == '__main__':
    run()
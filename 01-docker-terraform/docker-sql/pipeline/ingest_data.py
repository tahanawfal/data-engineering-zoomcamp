#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import argparse
import os

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


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    
    # Determine file type from URL or path
    if url.endswith('.parquet'):
        # Handle parquet files
        df = pd.read_parquet(url)
        
        df.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )
        print(f"Table {target_table} created")
        
        # Insert in chunks
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )
            print(f"Inserted chunk {i//chunksize + 1}: {len(chunk)} rows")
    else:
        # Handle CSV files
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )

        first_chunk = next(df_iter)

        first_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )

        print(f"Table {target_table} created")

        first_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print(f"Inserted first chunk: {len(first_chunk)}")

        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )
            print(f"Inserted chunk: {len(df_chunk)}")

    print(f'done ingesting to {target_table}')

def main():
    parser = argparse.ArgumentParser(description='Ingest taxi data into PostgreSQL')
    
    # Database parameters
    parser.add_argument('--pg-user', default=os.getenv('PG_USER', 'root'), help='PostgreSQL user')
    parser.add_argument('--pg-pass', default=os.getenv('PG_PASSWORD', 'root'), help='PostgreSQL password')
    parser.add_argument('--pg-host', default=os.getenv('PG_HOST', 'pgdatabase'), help='PostgreSQL host')
    parser.add_argument('--pg-port', default=os.getenv('PG_PORT', '5432'), help='PostgreSQL port')
    parser.add_argument('--pg-db', default=os.getenv('PG_DB', 'ny_taxi'), help='PostgreSQL database')
    
    # Data parameters
    parser.add_argument('--year', type=int, default=2025, help='Year of data')
    parser.add_argument('--month', type=int, default=11, help='Month of data')
    parser.add_argument('--target-table', default='green_taxi_data', help='Target table name')
    parser.add_argument('--chunksize', type=int, default=100000, help='Chunk size for ingestion')
    parser.add_argument('--data-type', choices=['yellow', 'green', 'fhv', 'fhvhv'], default='green', help='Type of taxi data')
    parser.add_argument('--file-format', choices=['csv', 'parquet'], default='parquet', help='File format')
    parser.add_argument('--url', default=None, help='Custom URL to fetch data from')
    
    args = parser.parse_args()

    pg_user = args.pg_user
    pg_pass = args.pg_pass
    pg_host = args.pg_host
    pg_port = args.pg_port
    pg_db = args.pg_db
    year = args.year
    month = args.month
    chunksize = args.chunksize
    target_table = args.target_table
    data_type = args.data_type
    file_format = args.file_format

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    if args.url:
        url = args.url
    else:
        # Build URL based on data type and format
        if file_format == 'parquet':
            url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{data_type}_tripdata_{year:04d}-{month:02d}.parquet'
        else:
            url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
            url = f'{url_prefix}/{data_type}/{data_type}_tripdata_{year:04d}-{month:02d}.csv.gz'

    print(f"Connecting to PostgreSQL at {pg_host}:{pg_port}/{pg_db}")
    print(f"Fetching data from: {url}")
    
    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()
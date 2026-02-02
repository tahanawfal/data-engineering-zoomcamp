#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import argparse
import os
import sys

def ingest_data(url: str, engine, target_table: str, chunksize: int = 100000):
    """Ingest data from URL to PostgreSQL table in chunks."""
    
    try:
        print(f"Fetching data from: {url}")
        
        # Read data based on file format
        if url.endswith('.parquet'):
            df = pd.read_parquet(url)
            total_rows = len(df)
        else:
            # For CSV, read first chunk to create table
            df_iter = pd.read_csv(url, iterator=True, chunksize=chunksize)
            df = next(df_iter)
            total_rows = "unknown"
        
        # Create table with schema from first chunk
        df.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
        print(f"Table '{target_table}' created")
        
        # Insert data in chunks
        rows_inserted = 0
        
        if url.endswith('.parquet'):
            # Chunk the parquet dataframe
            chunks = [df.iloc[i:i+chunksize] for i in range(0, len(df), chunksize)]
            for chunk in tqdm(chunks, desc="Ingesting"):
                chunk.to_sql(name=target_table, con=engine, if_exists="append")
                rows_inserted += len(chunk)
        else:
            # Process CSV iterator
            df.to_sql(name=target_table, con=engine, if_exists="append")
            rows_inserted += len(df)
            
            for chunk in tqdm(df_iter, desc="Ingesting"):
                chunk.to_sql(name=target_table, con=engine, if_exists="append")
                rows_inserted += len(chunk)
        
        print(f"✓ Successfully ingested {rows_inserted} rows into '{target_table}'")
        
    except Exception as e:
        print(f"✗ Error during ingestion: {e}", file=sys.stderr)
        raise

def main():
    parser = argparse.ArgumentParser(description='Ingest taxi data into PostgreSQL')
    
    # Database parameters
    parser.add_argument('--pg-user', default=os.getenv('PG_USER', 'root'))
    parser.add_argument('--pg-pass', default=os.getenv('PG_PASSWORD', 'root'))
    parser.add_argument('--pg-host', default=os.getenv('PG_HOST', 'pgdatabase'))
    parser.add_argument('--pg-port', default=os.getenv('PG_PORT', '5432'))
    parser.add_argument('--pg-db', default=os.getenv('PG_DB', 'ny_taxi'))
    
    # Data parameters
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--month', type=int, default=1)
    parser.add_argument('--target-table', default='taxi_data')
    parser.add_argument('--chunksize', type=int, default=100000)
    parser.add_argument('--data-type', choices=['yellow', 'green', 'fhv', 'fhvhv'], default='green')
    parser.add_argument('--file-format', choices=['csv', 'parquet'], default='parquet')
    parser.add_argument('--url', help='Custom URL (overrides auto-generated URL)')
    
    args = parser.parse_args()
    
    # Build database connection string
    engine = create_engine(
        f'postgresql://{args.pg_user}:{args.pg_pass}@{args.pg_host}:{args.pg_port}/{args.pg_db}'
    )
    
    # Determine data URL
    if args.url:
        url = args.url
    else:
        if args.file_format == 'parquet':
            url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{args.data_type}_tripdata_{args.year:04d}-{args.month:02d}.parquet'
        else:
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{args.data_type}/{args.data_type}_tripdata_{args.year:04d}-{args.month:02d}.csv.gz'
    
    print(f"Database: {args.pg_host}:{args.pg_port}/{args.pg_db}")
    
    ingest_data(url=url, engine=engine, target_table=args.target_table, chunksize=args.chunksize)

if __name__ == '__main__':
    main()
import os


import os
import argparse
from ast import parse
import pandas as pd
from time import time
from sqlalchemy import create_engine

def main(params):
    # Reading in the parameters from 
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # Download the data from source
    os.system(f"wget {url} -O {csv_name}")

    # Creating engine to connect to database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # Reading in the yellow taxi data
    df_iter = pd.read_csv(csv_name, iterator=True,
                        chunksize=100000)

    # Reading first chunk from iterator
    df = next(df_iter)

    # Fixing timestamps
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Creating table headers
    df.to_sql(name=table_name, con=engine, if_exists='replace')

    # Loop through all the chunks of dataframe and add to database
    i = 0
    while True:
        start = time()
        
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        end = time()
        i += 1
        print(f'Inserted chunk {i}, which took %.3f seconds' % (end - start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres ')
    parser.add_argument('--user',help='Postgres username')
    parser.add_argument('--password', help='Password for Postgres')
    parser.add_argument('--host', help='Host for Postgres')
    parser.add_argument('--port', help='Port for Postgres')
    parser.add_argument('--db', help='Database name for Postgres')
    parser.add_argument('--table_name', help='Name of table to write to')
    parser.add_argument('--url', help='Path to csv file')

    args = parser.parse_args()

    main(args)

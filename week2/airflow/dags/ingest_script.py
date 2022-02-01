import os
from ast import parse
import pandas as pd
from time import time
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_name):
    """
    This function will connect to a database and load a csv file into a table.
    """

    # Creating engine to connect to database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print('connection established successfully')
    
    start = time()

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
    end = time() 
    print(f'Inserted first chunk, which took %.3f seconds' % (end - start))

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
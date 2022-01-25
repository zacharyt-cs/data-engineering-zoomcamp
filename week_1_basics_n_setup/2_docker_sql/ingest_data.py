#!/usr/bin/env python
# coding: utf-8

from fileinput import filename
import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_taxi = params.table_taxi
    table_zone = params.table_zone
    url_taxi = params.url_taxi
    url_zone = params.url_zone
    taxi_csv = 'taxi.csv'
    zone_csv = 'zone.csv'
   
    if not os.path.isfile(taxi_csv):
        os.system(f"wget {url_taxi} -O {taxi_csv}")

    if not os.path.isfile(zone_csv):
        os.system(f"wget {url_zone} -O {zone_csv}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(zone_csv)
    df.head(n=0).to_sql(name=table_zone, con=engine, if_exists='replace')
    df.to_sql(name=table_zone, con=engine, if_exists='append')

    df_iter = pd.read_csv(taxi_csv, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_taxi, con=engine, if_exists='replace')

    df.to_sql(name=table_taxi, con=engine, if_exists='append')


    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_taxi, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_taxi', help='name of the table where we will write the results to')
    parser.add_argument('--table_zone', help='name of the table where we will write the results to')
    parser.add_argument('--url_taxi', help='url of the csv file')
    parser.add_argument('--url_zone', help='url of the csv file')

    args = parser.parse_args()

    main(args)
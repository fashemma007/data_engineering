# from sqlalchemy import create_engine
# from time import time
# import pandas as pd
# import os
# from pathlib import Path

# # PG_HOST=os.getenv('PG_HOST')
# # PG_USER=os.getenv('PG_USER')
# # PG_PASSWORD=os.getenv('PG_PASSWORD')
# # PG_PORT=os.getenv('PG_PORT')
# # PG_DATABASE=os.getenv('PG_DATABASE')

# #=========PARAMS==========
# # user,
# # password,
# # host,
# # port,
# # database name,
# # table name,
# # url of CSV

# """
# URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
# python ingest_data.py \
#   --user=root \
#   --password=password \
#   --host=localhost \
#   --port=5431 \
#   --db=ny_taxi \
#   --table_name=yellow_taxi_trips \
#   --url=${URL}
# """

# def ingest_callable(user, password, host, port, db, table_name,csv_file):
   
#    # download the csv file
   
#   #  csv_name = csv_file
#   #  file_name = Path("output.csv")
#   #  print (table_name,csv_name)
#    # if file_name.exists():
#    #    pass
#    # else:
#    #    f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE}'
   
#    # Initializing our connection to the server
#    # engine = create_engine('postgresql://root:password@localhost:5431/ny_taxi')
#    # engine.connect()
   
#    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
#    engine.connect()
#    t_start = time()
#   #  #==============================================================
#   #  #           UPLOADING ZONES LOOK_UP FILE
#   #  #==============================================================
#   #  zone_file_name = "zone_lookup.csv"
#   #  zone_file = Path("zone_lookup.csv")
#   #  zone_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
#   #  if zone_file.exists():
#   #     pass
#   #  else:
#   #     os.system(f"wget {zone_url} -O {zone_file_name}")
   
#   #  df_zones = pd.read_csv(zone_file_name)
#   #  df_zones.to_sql(name='zones',con=engine,if_exists='replace')
#   #============================================================== 
   
#    # to split our full data into chunks
#    dtf_iter = pd.read_csv(csv_file,iterator=True, chunksize =100000)
   
#    dtf = next(dtf_iter)
#    # to convert the data in the drop off and pickup columns to date time
#    dtf.tpep_dropoff_datetime = pd.to_datetime(dtf.tpep_dropoff_datetime)   
#    dtf.tpep_pickup_datetime = pd.to_datetime(dtf.tpep_pickup_datetime)   
   
#    dtf.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
   
#    #Importing our first 100k rows before iterator
#    dtf.to_sql(name=table_name,con=engine,if_exists='append')
   
#    while True:
#       try:
#          t_start = time()
#          dtf = next(dtf_iter)
         
#          # to convert the data in the drop off and pickup columns to date time
#          dtf.tpep_dropoff_datetime = pd.to_datetime(dtf.tpep_dropoff_datetime)
         
#          dtf.tpep_pickup_datetime = pd.to_datetime(dtf.tpep_pickup_datetime)   
   
      
#          dtf.to_sql(name=table_name,con=engine,if_exists='append')
#          t_end = time()
#          diff = t_end - t_start 
#          print (f'Another 100k rows of data uploaded in %.3f seconds' %(diff))
#       except StopIteration:
#          print ('Done uploading')
#          break

# # cursor = engine.connect() # creating a cursor to run sql queries with
# # cursor.execute("""CREATE TABLE yellow_taxi_data (
# # 	"VendorID" BIGINT, 
# # 	tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
# # 	tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
# # 	passenger_count BIGINT, 
# # 	trip_distance FLOAT(53), 
# # 	"RatecodeID" BIGINT, 
# # 	store_and_fwd_flag TEXT, 
# # 	"PULocationID" BIGINT, 
# # 	"DOLocationID" BIGINT, 
# # 	payment_type BIGINT, 
# # 	fare_amount FLOAT(53), 
# # 	extra FLOAT(53), 
# # 	mta_tax FLOAT(53), 
# # 	tip_amount FLOAT(53), 
# # 	tolls_amount FLOAT(53), 
# # 	improvement_surcharge FLOAT(53), 
# # 	total_amount FLOAT(53), 
# # 	congestion_surcharge FLOAT(53)
# # )""")


import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('Connection established successfully. \n Inserting data...')

    t_start = time()
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('Inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted another chunk, took %.3f second' % (t_end - t_start))
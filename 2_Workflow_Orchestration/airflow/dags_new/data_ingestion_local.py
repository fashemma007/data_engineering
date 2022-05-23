from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from ingest import ingest_callable,format_to_csv,clean_directory
from pathlib import Path

PG_HOST = os.environ.get("PG_HOST") 
PG_USER =os.environ.get("PG_USER") 
PG_PASSWORD=os.environ.get("PG_PASSWORD") 
PG_PORT=os.environ.get("PG_PORT") 
PG_DATABASE=os.environ.get("PG_DATABASE") 
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"

URL_TEMPLATE = URL_PREFIX+"yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
OUTPUT_FILE = AIRFLOW_HOME+'/yellow_taxi_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
TABLE_NAME = "yellow_taxi_{{execution_date.strftime(\'%Y_%m\')}}"
OUTPUT_CSV = AIRFLOW_HOME+'/yellow_taxi_{{execution_date.strftime(\'%Y-%m\')}}.csv'

# URL_TEMPLATE = URL_PREFIX+"fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
# OUTPUT_FILE = AIRFLOW_HOME+'/fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
# TABLE_NAME = "fhv_tripdata_{{execution_date.strftime(\'%Y_%m\')}}"

# URL_TEMPLATE="http://172.29.112.1:8000/nyc_tripdata_2022-05.csv"


#Declaring the DAG object
local_workflow = DAG(
   "Local_ingestion", #DAG name 
   schedule_interval="0 0 1 * *",#“At 00:00 on day-of-month 2.”
   start_date=datetime(2019, 1, 1), #DAG start date
   end_date=datetime(2020, 12, 1),#DAG end date
   catchup=True
   )

# Now to create tasks under the DAG
with local_workflow:
   wget_task = BashOperator(
      task_id="get_parquet_files",
      bash_command= f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE}',
      # bash_command= f'echo Successfully downloaded to yellow_taxi_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
   ) #Create a bash operators/command line task to execute "bash command"
   
   parquet_to_csv_task = PythonOperator(
      task_id="parquet_to_csv",   
      python_callable=format_to_csv,
      op_kwargs=dict(
         src_file=OUTPUT_FILE,
         output_csv=OUTPUT_CSV
      )
   )
   
   ingest_task = PythonOperator(
      task_id="ingest_to_postgres",
      python_callable= ingest_callable,
      op_kwargs=dict(
         user=PG_USER,
         password=PG_PASSWORD,
         host=PG_HOST, 
         port=PG_PORT,
         db=PG_DATABASE,
         table_name=TABLE_NAME,
         csv_file=OUTPUT_CSV)
   )
   delete_parquet = PythonOperator(
      task_id="delete_parquet",   
      python_callable=clean_directory,
      op_kwargs=dict(
         parquet=OUTPUT_FILE,
         csv=OUTPUT_CSV
        
      )
   )
   wget_task>>parquet_to_csv_task>>ingest_task>>delete_parquet
   
   
   # 'echo "{{execution_date.strftime(\'%Y-%m\')}}
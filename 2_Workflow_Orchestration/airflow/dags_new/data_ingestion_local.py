from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from ingest import ingest_callable
from pathlib import Path

PG_HOST = os.environ.get("PG_HOST") 
PG_USER =os.environ.get("PG_USER") 
PG_PASSWORD=os.environ.get("PG_PASSWORD") 
PG_PORT=os.environ.get("PG_PORT") 
PG_DATABASE=os.environ.get("PG_DATABASE") 
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
URL_TEMPLATE = URL_PREFIX+"yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
# URL_TEMPLATE="file:///C:/Users/efaso/Documents/git_repos/data_engineering/1_Docker_Terraform_GCP_Intro/output.csv"
OUTPUT_FILE = AIRFLOW_HOME+'/nyc_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
TABLE_NAME = "yellow_taxi_{{execution_date.strftime(\'%Y_%m\')}}"

#Declaring the DAG object
local_workflow = DAG(
   "Local_ingestion", #DAG name 
   schedule_interval="0 6 * * 1",#6am on Mondays
   start_date=datetime(2022,5,1) #DAG start date
   )

# Now to create tasks under the DAG
with local_workflow:
   wget_task = BashOperator(
      task_id="wget",
      # bash_command= f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE}'
      bash_command = 'echo "{{execution_date.strftime(\'%Y-%m\')}}"'
   ) #Create a bash operators/command line task to execute "bash command"
   
   ingest_task = PythonOperator(
      task_id="ingest",
      python_callable= ingest_callable,
      op_kwargs=dict(
         user=PG_USER,
         password=PG_PASSWORD,
         host=PG_HOST, 
         port=PG_PORT,
         db=PG_DATABASE,
         table_name=TABLE_NAME,
         csv_file=OUTPUT_FILE)
   )
   
   wget_task>>ingest_task
   
   
   # 'echo "{{execution_date.strftime(\'%Y-%m\')}}
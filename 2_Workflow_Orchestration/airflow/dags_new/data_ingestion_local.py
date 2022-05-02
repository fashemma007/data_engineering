from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pathlib import Path

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
URL_TEMPLATE = URL_PREFIX+"yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
OUTPUT_FILE = AIRFLOW_HOME+'/nyc_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'

#Declaring the DAG object
local_workflow = DAG(
   "Local_ingestion", #DAG name 
   schedule_interval="0 6 * * *",#6am daily
   start_date=datetime(2022,5,1) #DAG start date
   )

# Now to create tasks under the DAG
with local_workflow:
   wget_task = BashOperator(
      task_id="wget",
      bash_command= f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE}'
      # bash_command = 'echo "{{execution_date.strftime(\'%Y-%m\')}}"'
   ) #Create a bash operators/command line task to execute "bash command"
   
   ingest_task = BashOperator(
      task_id="ingest",
      bash_command= f'ls -lh {AIRFLOW_HOME}'
   )
   
   wget_task>>ingest_task
   
   
   # 'echo "{{execution_date.strftime(\'%Y-%m\')}}
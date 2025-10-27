from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import tasks
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json 

## Now we define the DAG 

with DAG (
    dag_id = "etl_learn_proj", 
    start_date = days_ago(1), 
    schedule = "@daily", 
    catchup = False
) as dag:
    
    ## Step 1: Create a table if it doesnot exist 
    
    
    
    ## Step 2: Extract: Any API 
    
    
    
    
    ## Step 3: Transform: Picking the info that I need to save
    
    
    
    ## Step 4: Load: Into the Postgres SQL 
    
    
    
    
    ## Step 5: Verify the dbViewer
    
    
    
    ## Step 6: Define the task dependencies (final)
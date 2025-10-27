from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
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
    @task 
    def create_table():
        #! init the postgres hook 
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        
        #! SQL query t create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY, 
            title VARCHAR(255), 
            explanation TEXT, 
            url TEXT, 
            date DATE, 
            media_type VARCHAR(50)
        );
        
        """
        
        #! Execute the table creation query
        postgres_hook.run(create_table_query)
        
    ## Step 2: Extract: Any API 
    
    
    
    
    ## Step 3: Transform: Picking the info that I need to save
    
    
    
    ## Step 4: Load: Into the Postgres SQL 
    
    
    
    
    ## Step 5: Verify the dbViewer
    
    
    
    ## Step 6: Define the task dependencies (final)
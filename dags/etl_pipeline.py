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
    
    extract = SimpleHttpOperator(
        task_id = 'extract_apod', 
        http_conn_id = 'nasa_api', ##! This is the connection id defined in Airflow
        endpoint = 'planetary/apod', 
        method= 'GET', 
        data = {"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"}, 
        response_filter = lambda response: response.jsoon()
    )
    
    
    ## Step 3: Transform: Picking the info that I need to save
    @task 
    def transform_data(response):
        apod_data = {
            'title': response.get('title', ''), 
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data
    
    
    ## Step 4: Load: Into the Postgres SQL 
    @task
    def load_into_db(apod_data):
        #! hook into the postgres
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        
        #! then we insert the data to the table
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s);
        """

        #! Then we execute the SQL query 
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
    
    
    ## Step 5: Verify the dbViewer
    
    
    
    ## Step 6: Define the task dependencies (final)
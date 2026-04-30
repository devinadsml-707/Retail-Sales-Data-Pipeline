"""
retail_etl_dag.py
=================
Airflow DAG — Retail Sales ETL Pipeline

Automates the end-to-end data pipeline:
  1. Extract raw retail transaction data from PostgreSQL
  2. Clean and normalize the data
  3. Bulk-index into Elasticsearch for Kibana dashboards

Dataset: Istanbul shopping mall retail transactions (99,457 rows)
Author : Devina Agustina
Schedule: Every Saturday at 09:10, 09:20, 09:30 UTC
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#TASK 1: Fetch from PostgreSQL
def fetch_from_postgresql():
    """
    Purpose: to connects to a PostgreSQL database, retrieves all records from the 'table_m3' table, and saves the data as a CSV file.
    Input: - (only contain database connection parameter)
    Output: -
    Notes: the output file will be overwritten if the file already exists.
    """
    import pandas as pd
    import psycopg2

    connection = psycopg2.connect(
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432",            
        database="milestone3"
    )
    df = pd.read_sql('SELECT * FROM table_m3', connection)
    df.to_csv('/tmp/retail_transactions_raw.csv', index=False)
    connection.close()
    print(f"Fetched {len(df)} rows from PostgreSQL")

#TASK 2: Data Cleaning
def data_cleaning():
    """
    Purpose: to perform a complete end-to-end data cleaning pipeline on the raw DataFrame 
    fetched from PostgreSQL. This function combines all cleaning steps into a single callable 
    function to ensure consistency, reproducibility, and ease of use within the Airflow DAG.
    
    Input: raw pandas DataFrame fetched from PostgreSQL table_m3
    Output: fully cleaned pandas DataFrame saved as 'retail_transactions_clean.csv'
    
    Steps to clean the data:
    1. Normalize column names
       - Convert all column names to lowercase
       - Strip leading/trailing whitespace
       - Replace spaces with underscores
       - Remove special symbols
    2. Remove duplicate rows
       - A row is considered duplicate if ALL column values are identical
       - This step is present even if no duplicates exist as a safeguard
    3. Handle missing values
       - Numerical columns → filled with median
       - Categorical columns → filled with mode
       - This step is present even if no missing values exist as a safeguard
    4. Fix data types
       - age         : float  → int
       - quantity    : float  → int
       - price       : float  → float (2 decimals)
       - invoice_date: object → datetime
    5. Save clean data to CSV (index=False)
    """
    import pandas as pd

    df = pd.read_csv('/tmp/retail_transactions_raw.csv')
    print(df.head(5))

    # Normalize column names
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(' ', '_', regex=False)
        .str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
    )

    # Remove duplicates
    df = df.drop_duplicates()

    # Handle missing values
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].fillna(df[col].median())
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].fillna(df[col].mode()[0])

    # Fix data types
    df['age']          = df['age'].astype(int)
    df['quantity']     = df['quantity'].astype(int)
    df['price']        = df['price'].astype(float).round(2)
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])

    # Save clean CSV
    df.to_csv('/tmp/retail_transactions_clean.csv', index=False)
    print(f"Data cleaned! Shape: {df.shape}")

#TASK 3: Post to Elasticsearch (Bulk Indexing)
def post_to_elasticsearch():
    """
    Purpose:to connects to an Elasticsearch, reads cleaned data from a CSV file, and do bulk indexing of the data into the specified Elasticsearch index.
    Input: None (Inputing data into elasticsearch, elasticsearch host URL, index name, and file path are hardcoded in this function.)
    Output: printing the number of successful and failed index operation
    Notes:
        - Assumes Elasticsearch is running and accessible at 'http://elasticsearch:9200'.
        - Reads data from '/tmp/retail_transactions_clean.csv'; ensure the file exists.
        - NaN values are converted to None to ensure compatibility with Elasticsearch.
        - Uses bulk indexing for efficiency, require batching or chunking for optimal performance.
    """
    import pandas as pd
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk

    # Connect
    es = Elasticsearch('http://elasticsearch:9200')

    # Verify connection
    if not es.ping():
        raise ValueError("Cannot connect to Elasticsearch!")
    print("Connected to Elasticsearch!")

    # Load clean data
    df = pd.read_csv('/tmp/retail_transactions_clean.csv')
    print(f"Loaded {len(df)} rows for indexing")

    # Replace NaN with None for ES compatibility
    df = df.where(pd.notnull(df), None)

    # Prepare bulk actions
    actions = [
        {
            '_index' : 'retail_transactions',
            '_id'    : i,
            '_source': row.to_dict()
        }
        for i, row in df.iterrows()
    ]

    # Execute bulk indexing
    success, failed = bulk(es, actions, raise_on_error=False)

    print(f"Bulk indexing complete!")
    print(f"Successfully indexed : {success} documents")
    print(f"Failed               : {len(failed)} documents")

#DAG DEFINITION
with DAG(
    dag_id='retail_etl_pipeline',
    start_date=datetime(2024, 11, 1),
    schedule_interval='10,20,30 9 * * 6',
    catchup=False,
    default_args={
        'owner': 'devina',
        'retries': 1,
    },
    description='Retail ETL: fetch from PostgreSQL → clean → load to Elasticsearch'
) as dag:

    task1 = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_from_postgresql
    )

    task2 = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning
    )

    task3 = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    task1 >> task2 >> task3
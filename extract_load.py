
import pyarrow
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import os


PROJECT_ID = "nyc-311-pipeline-512717"
TABLE_ID = f"{PROJECT_ID}.bronze_layer.raw_nyc_311"

def fetch_yesterdays_data():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT00:00:00.000')
    url = f"https://data.cityofnewyork.us/resource/erm2-nwe9.json?$where=created_date >= '{yesterday}'&$limit=50000"
    
    response = requests.get(url)
    response.raise_for_status()
    
    return pd.DataFrame(response.json())

def load_to_bigquery(df):
    if df.empty:
        return
        
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True 
    )
    
    df = df.astype(str)
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result() 

if __name__ == "__main__":
    raw_data_df = fetch_yesterdays_data()
    load_to_bigquery(raw_data_df)
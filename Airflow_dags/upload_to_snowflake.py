
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from datetime import datetime
import logging
# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}
dag = DAG(
    'ETL',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger for now
)
# Extracting the File paths
CSV_FILES ={
    "users": "/mnt/c/Users/harik/Downloads/fetch/structured_users.csv",
    "brands": "/mnt/c/Users/harik/Downloads/fetch/structured_brands.csv",
    "receipts": "/mnt/c/Users/harik/Downloads/fetch/normalized_receipts.csv",
    "items": "/mnt/c/Users/harik/Downloads/fetch/normalized_items.csv",
    "products": "/mnt/c/Users/harik/Downloads/fetch/normalized_products.csv",
    "rewards": "/mnt/c/Users/harik/Downloads/fetch/normalized_rewards.csv",
    "user_flagged": "/mnt/c/Users/harik/Downloads/fetch/normalized_user_flagged.csv",
    "metabrite": "/mnt/c/Users/harik/Downloads/fetch/normalized_metabrite.csv"
}

# Snowflake table mappings
SNOWFLAKE_TABLES = {
    "users": "ANALYTICS.PUBLIC.users",
    "brands": "ANALYTICS.PUBLIC.brands",
    "receipts": "ANALYTICS.PUBLIC.receipts",
    "products": "ANALYTICS.PUBLIC.products",
    "rewards": "ANALYTICS.PUBLIC.rewards",
    "user_flagged": "ANALYTICS.PUBLIC.user_flagged_items",
    "items": "ANALYTICS.PUBLIC.receipt_items",
    "metabrite" : "ANALYTICS.PUBLIC.metabrite_items"
}

def convert_value(value):
    if pd.isna(value):  # Handle NaN values (NULL in Snowflake)
        return None
    elif isinstance(value, (int, float, bool)):  # Keep numbers and booleans unchanged
        return value
    elif isinstance(value, pd.Timestamp):  # Convert Pandas Timestamp to Snowflake format
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, datetime):  # Convert Python datetime to string format
        return value.strftime('%Y-%m-%d %H:%M:%S')
    else:  # Convert everything else to a string
        return str(value)

#Uploading CSV to snowflake 
#This creates tables
def upload_to_snowflake(file_key):
    ''' Uploading CSV files to snowflake using COPY INTO command.'''
    snowflake_hook=SnowflakeHook(snowflake_conn_id='snowflake_default_final')
    stage_name='my_internal_stage'
    file_path=CSV_FILES[file_key]
    #uploading files to stage
    put_command=f"PUT 'file://{file_path}'@{stage_name};"
    try:
        snowflake_hook.run(put_command)
        logging.info(f"File {file_path} successfully staged in {stage_name}.")
         # COPY INTO command to load data from stage to table
        copy_command = f"""
        COPY INTO {SNOWFLAKE_TABLES[file_key]}
        FROM @{stage_name}/{file_key}.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
        """
        snowflake_hook.run(copy_command)
        logging.info(f"Data successfully loaded into {SNOWFLAKE_TABLES[file_key]} using COPY INTO.")
    except Exception as e:
        logging.error(f"Error occurred while loading data for {file_key}: {str(e)}")

    #  Ensure datetime columns are in correct format
    #for col in df.columns:
        #if 'date' in col.lower() or 'time' in col.lower():
            #df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert to datetime


# Defining Airflow Tasks
for file_key in CSV_FILES.keys():
    upload_task = PythonOperator(
        task_id=f'upload_snowflake_{file_key}', 
        python_callable=upload_to_snowflake, 
        op_kwargs={'file_key': file_key}, 
        dag=dag
    )








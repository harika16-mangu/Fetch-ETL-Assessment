
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from datetime import datetime
import logging
# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}
dag = DAG(
    'ETL',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger for now
)
# Extracting the File paths
CSV_FILES ={
    #"users": "/mnt/c/Users/harik/Downloads/fetch/structured_users.csv",
    #"brands": "/mnt/c/Users/harik/Downloads/fetch/structured_brands.csv",
    #"receipts": "/mnt/c/Users/harik/Downloads/fetch/normalized_receipts.csv",
    #"items": "/mnt/c/Users/harik/Downloads/fetch/normalized_items.csv",
    "products": "/mnt/c/Users/harik/Downloads/fetch/normalized_products.csv",
    #"rewards": "/mnt/c/Users/harik/Downloads/fetch/normalized_rewards.csv",
    #"user_flagged": "/mnt/c/Users/harik/Downloads/fetch/normalized_user_flagged.csv",
    #"metabrite": "/mnt/c/Users/harik/Downloads/fetch/normalized_metabrite.csv"
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
    snowflake_hook=SnowflakeHook(snowflake_conn_id='snowflake_default_final')
    df=pd.read_csv(CSV_FILES[file_key])
    #  Ensure datetime columns are in correct format
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert to datetime

    # Convert data to correct types before sending to Snowflake
    data = [tuple(convert_value(value) for value in row) for row in df.itertuples(index=False, name=None)]
    
    if data:
        logging.info(f"First row of {file_key}: {data[0]}")
    else:
        logging.warning(f"WARNING: No data found for {file_key}!")

#Insering the values into the tables
    if file_key=="users":
        query=f"INSERT INTO {SNOWFLAKE_TABLES[file_key]} (user_id,active,role,signUpSource,state,createdDate,lastLogin) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    elif file_key=="brands":
        query=f"Insert into {SNOWFLAKE_TABLES[file_key]}(brand_id,barcode,brandCode,category,categoryCode,cpg_id,name,topBrand) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    elif file_key=="receipts":
        query=f"Insert into {SNOWFLAKE_TABLES[file_key]} (receipt_id,userId,bonusPointsEarned,pointsEarned,purchasedItemCount,totalSpent,rewardsReceiptStatus,createDate,dateScanned,finishedDate,modifyDate,pointsAwardedDate,purchaseDate) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif file_key=="products":
        query=f"insert into {SNOWFLAKE_TABLES[file_key]} (barcode,description,itemPrice,targetPrice,discountedItemPrice,finalPrice,competitiveProduct,needsFetchReview,needsFetchReviewReason,product_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif file_key=='rewards':
        query=f"insert into {SNOWFLAKE_TABLES[file_key]} (rewardsProductPartnerId,rewardsGroup,pointsEarned,pointsPayerId,pointsNotAwardedReason,reward_id) values (%s,%s,%s,%s,%s,%s)"
    elif file_key=="user_flagged":
        query= f"insert into {SNOWFLAKE_TABLES[file_key]} (userFlaggedBarcode,userFlaggedDescription,userFlaggedPrice,userFlaggedQuantity,userFlaggedNewItem,user_flagged_id) values (%s,%s,%s,%s,%s,%s)"
    elif file_key=="items":
        query= f"insert into {SNOWFLAKE_TABLES[file_key]} (receipt_id,barcode,partnerItemId,preventTargetGapPoints,quantityPurchased,userFlaggedBarcode,originalReceiptItemText,originalFinalPrice,originalMetaBriteBarcode,rewardsProductPartnerId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif file_key=="metabrite":
        query= f"insert into {SNOWFLAKE_TABLES[file_key]} (originalMetaBriteBarcode,originalMetaBriteDescription,originalMetaBriteItemPrice,originalMetaBriteQuantityPurchased,metabrite_id) values (%s,%s,%s,%s,%s)"


    for row in data:
        snowflake_hook.run(query,parameters=row)

# Defining Airflow Tasks
for file_key in CSV_FILES.keys():
    upload_task = PythonOperator(
        task_id=f'upload_snowflake_{file_key}', 
        python_callable=upload_to_snowflake, 
        op_kwargs={'file_key': file_key}, 
        dag=dag
    )








import pandas as pd
import boto3
import numpy as np
import pytz
import urllib
import math
import json
import time
from io import BytesIO
import snowflake.connector
from sqlalchemy.sql import text
from sqlalchemy.types import VARCHAR
from datetime import datetime,timedelta,timezone,date
import requests
from sqlalchemy import create_engine
from decimal import Decimal
from urllib.parse import quote
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key,load_der_private_key

def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['snowflake_bizops_user','snowflake_account','snowflake_salesmarketing_schema','snowflake_fivetran_db','snowflake_bizops_role','snowflake_key_pass','snowflake_bizops_wh','docebo_url','docebo_client_id','docebo_secret_id','docebo_lp_id','docebo_courses_id']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_schema = extracted_secrets['snowflake_salesmarketing_schema']['snowflake_salesmarketing_schema']
snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']
docebo_client_id = extracted_secrets['docebo_client_id']['docebo_client_id']
docebo_secret_id = extracted_secrets['docebo_secret_id']['docebo_secret_id']
docebo_lp_id = extracted_secrets['docebo_lp_id']['docebo_lp_id']
docebo_courses_id = extracted_secrets['docebo_courses_id']['docebo_courses_id']
docebo_url = extracted_secrets['docebo_url']['docebo_url']

password = snowflake_key_pass.encode()

# AWS S3 Configuration
s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

# Function to download file from S3
def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

# Download the private key file from S3
key_data = download_from_s3(s3_bucket, s3_key)

password = 'hhaexchange'.encode()  # Convert password to bytes

# Try loading the private key as PEM
private_key = load_pem_private_key(key_data, password=password)

# Extract the private key bytes in PKCS8 format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)


client_id = docebo_client_id
client_secret = docebo_secret_id

auth_url = f'https://{docebo_url}/oauth2/token'

payload = {
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type':'client_credentials',
    'scope':'api'
}

auth_response = requests.post(auth_url, data=payload)

if auth_response.status_code == 200:
    token = auth_response.json().get('access_token')
else:
    print("Failed to obtain token. Status code:", auth_response.status_code)

lp = docebo_lp_id
courses = docebo_client_id

url = f'https://{docebo_url}/analytics/v1/reports/{lp}/export/csv'

headers = {
'Accept':'application/json',
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'en-US,en;q=0.9',
'Authorization': f'Bearer {token}'
}

has_more_data = True
current_page = 1
course_ids = []

while has_more_data:
    params = {'page': current_page,}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get('data', {})
        execution_id = data.get('executionId', [])
        break

    else:
        break

time.sleep(300)

auth_response = requests.post(auth_url, data=payload)

if auth_response.status_code == 200:
    token = auth_response.json().get('access_token')
else:
    print("Failed to obtain token. Status code:", auth_response.status_code)

base_url = f'https://{docebo_url}/analytics/v1/reports/{lp}/exports/{execution_id}/results?pageSize=1000'

headers = {
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Authorization': f'Bearer {token}'
}

all_data = []
next_token = None

while True:
    url = f"{base_url}&nextToken={quote(next_token, safe='')}" if next_token else base_url

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        response_json = response.json()

        new_data = response_json.get('data', [])
        if new_data:
            all_data.extend(new_data)
        else:
            break

        next_token = response_json.get('nextToken')

        if not next_token:
            break
    else:
        break

time.sleep(300)

all_data_modified = [{k.replace(' ', '_'): v for k, v in entry.items()} for entry in all_data]
lp_df = pd.DataFrame(all_data_modified)
lp_df.columns = lp_df.columns.str.upper()
lp_df.rename(columns={'CREDITS_(CEUS)': 'CREDITS_CEU'}, inplace=True)
lp_df.replace('', np.nan, inplace=True)

auth_response = requests.post(auth_url, data=payload)

if auth_response.status_code == 200:
    token = auth_response.json().get('access_token')
else:
    print("Failed to obtain token. Status code:", auth_response.status_code)

url = f'https://{docebo_url}/analytics/v1/reports/{courses}/export/csv'

headers = {
'Accept':'application/json',
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'en-US,en;q=0.9',
'Authorization': f'Bearer {token}'
}

has_more_data = True
current_page = 1
course_ids = []

while has_more_data:
    params = {'page': current_page,}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get('data', {})
        execution_id = data.get('executionId', [])
        break

    else:
        print("Failed to retrieve data. Status code:", response.status_code)
        print("Response:", response.text)
        break # Exit the loop on failure

time.sleep(300)

base_url = f'https://{docebo_url}/analytics/v1/reports/{courses}/exports/{execution_id}/results?pageSize=1000'

headers = {
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Authorization': f'Bearer {token}'
}

all_data = []
next_token = None

while True:
    url = f"{base_url}&nextToken={quote(next_token, safe='')}" if next_token else base_url

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        response_json = response.json()

        new_data = response_json.get('data', [])
        if new_data:
            all_data.extend(new_data)
        else:
            print("No data found in response.")

        next_token = response_json.get('nextToken')
        
        if not next_token:
            break
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
        print("Response:", response.text)
        break

all_data_modified = [{k.replace(' ', '_'): v for k, v in entry.items()} for entry in all_data]
course_df = pd.DataFrame(all_data_modified)
course_df.columns = course_df.columns.str.upper()
course_df.rename(columns={'COURSE_PROGRESS_(%)': 'COURSE_PROGRESS_PERCENT','CREDITS_(CEUS)': 'CREDITS_CEU','INITIAL_LAUNCH_DATE:': 'INITIAL_LAUNCH_DATE','ARCHIVED_ENROLLMENT_(YES_/_NO)': 'ARCHIVED_ENROLLMENT','COURSE_PROGRESS_(%)': 'COURSE_PROGRESS_PERCENT','SESSION_TIME_(MIN)':'SESSION_TIME_MIN','TRAINING_MATERIAL_TIME_(SEC)':'TRAINING_MATERIAL_TIME_SEC','%_OF_TRAINING_MATERIAL_FROM_MOBILE_APP':'PERCENT_TRAINING_MATERIAL_FROM_MOBILE_APP'}, inplace=True)
course_df.replace('', np.nan, inplace=True)

connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/DOCEBO?warehouse={BIZOPS_ETL_WH}&role={BIZOPS_ETL_ROLE}&authenticator=externalbrowser"

engine = create_engine(
    connection_string,
    connect_args={
        "private_key": private_key_bytes})

ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh
)
    
cs = ctx.cursor()
script = """
delete from "PC_FIVETRAN_DB"."DOCEBO"."CUSTOM_LEARNING_PLAN";
delete from "PC_FIVETRAN_DB"."DOCEBO"."CUSTOM_COURSES"
"""
payload = cs.execute(script)

chunk_size = 10000
chunks = [x for x in range(0, len(course_df), chunk_size)] + [len(course_df)]
table_name = 'custom_courses' 

for i in range(len(chunks) - 1):
    print(f"Inserting rows {chunks[i]} to {chunks[i + 1]}")
    course_df[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

chunk_size = 10000
chunks = [x for x in range(0, len(lp_df), chunk_size)] + [len(lp_df)]
table_name = 'custom_learning_plan' 

engine = create_engine(
connection_string,
connect_args={
    "private_key": private_key_bytes})

for i in range(len(chunks) - 1):
    print(f"Inserting rows {chunks[i]} to {chunks[i + 1]}")
    lp_df[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

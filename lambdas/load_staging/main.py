"""
Ingest data from S3 formats to staging layer of BigQuery data warehouse
"""
from io import BytesIO
import pandas as pd
import boto3
import datetime
import json
from google.cloud import bigquery

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def lambda_handler(event=None, context=None):
    s3_bucket_name = 'monzo-data-bucket'
    
    s3_client = boto3.client('s3')
    
    bq_credentials = get_secret('monzo-bigquery-data-warehouse')
    bq_client = bigquery.Client.from_service_account_info(bq_credentials)

    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    """Retrieve data from S3 buckets - getting the most recently uploaded file"""
    # Get most recent file from each bucket subfolder
    """
    latest_files looks like this:
        {'transactions': 'transactions/transactions_2025-02-09-14-55-11.parquet',
        'balance': 'balance/balance_2025-02-09-14-55-11.json',
        'pots': 'pots/pots_2025-02-09-14-55-11.parquet'}
    """
    response = s3_client.list_objects_v2(Bucket='monzo-data-bucket')

    latest_files = {}

    for bucket in ['transactions', 'balance', 'pots']:
        bucket_files = [item for item in response.get('Contents') if bucket+'/' in item.get('Key')]
        latest_file = max(bucket_files, key=lambda x: x['LastModified'])
        latest_files[bucket] = latest_file.get('Key')

    try:
        # Retrieve transactions parquet
        transactions_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=latest_files['transactions'])
        
        # Parquet file read as BytesIO object - don't need to save to disc
        transactions_data = BytesIO(transactions_obj['Body'].read())
        transactions_df = pd.read_parquet(transactions_data)

        transactions_df['date_loaded'] = timestamp

        # Ensuring timestamp columns are correct before inserting to BQ
        for timestamp_col in ['created', 'settled', 'updated', 'date_loaded']:
            transactions_df[timestamp_col] = pd.to_datetime(transactions_df[timestamp_col], errors='coerce')
            transactions_df[timestamp_col] = transactions_df[timestamp_col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception as e:
        print(f'Error processing transactions data: {e}')
        raise

    try:
        # Getting balance data
        balance_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=latest_files['balance'])
        balance_json = json.loads(balance_obj['Body'].read())
        balance_json['date_loaded'] = timestamp
    except Exception as e:
        print(f'Error processing balance data: {e}')
        raise

    try:
        # Getting pots
        pots_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=latest_files['pots'])
        pots_data = BytesIO(pots_obj['Body'].read())
        pots_df = pd.read_parquet(pots_data)
        pots_df['date_loaded'] = timestamp
    except Exception as e:
        print(f'Error processing pots data: {e}')
        raise

    """Loading to staging tables"""

    try:
        transactions_staging_table = 'monzodatawarehouse.stg.raw_transactions'
        balance_staging_table = 'monzodatawarehouse.stg.raw_balance'
        pots_staging_table = 'monzodatawarehouse.stg.raw_pots'

        # Loading transactions_df into BQ staging table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = bq_client.load_table_from_dataframe(transactions_df, transactions_staging_table, job_config=job_config)
        load_job.result()

        # Loading balance_json to BQ staging table
        load_job = bq_client.load_table_from_json([balance_json], balance_staging_table)
        load_job.result()

        # Loading pots_df into BQ staging table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = bq_client.load_table_from_dataframe(pots_df, pots_staging_table, job_config=job_config)
        load_job.result()
    except Exception as e:
        print(f'Error loading data to BigQuery staging tables: {e}')
        raise

if __name__ == "__main__":
    lambda_handler(None, None)
"""
extract/main.py
This lambda function extract transactions, balance, pots from the Monzo API and loads them into S3 buckets.
Transactions data is converted to a dataframe then stored as parquet.
Balance is stored as json (tiny file).
Pots are stored as parquet.
"""
import pandas as pd
import datetime
import boto3
import json
from utils.api.api_client import MonzoAPIClient

def lambda_handler(event=None, context=None):
    # Bucket paths/names etc. Can replace with environment variables later if necessary.
    s3_bucket_name = 'monzo-data-bucket'

    # Create monzo API client
    client = MonzoAPIClient()

    # S3 client for loading to S3 buckets
    s3_client = boto3.client('s3')

    """Extracting data"""
    try:
        # # Optional date parameters (for backfilling transactions data)
        # start_date = '2024-12-01T00:00:00Z'
        # end_date = '2025-01-01T00:00:00Z'

        # Get transactions with client
        transactions = client.get_transactions(limit=100)

        # Get balance
        balance = client.get_balance()
        # Dict -> json
        balance = json.dumps(balance)

        # Get pots
        pots = client.get_pots()
        pots_df = pd.DataFrame(pots.get('pots'))
    except Exception as e:
        print(f'Error extracting data: {e}')
        raise

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    """Loading data to S3 buckets"""

    try:
        # Loading transactions data to S3
        transactions_df = pd.DataFrame(transactions.get('transactions'))

        # Convert json data to json string
        for json_col in ['fees', 'merchant', 'metadata', 'categories', 'counterparty']:
            transactions_df[json_col] = transactions_df[json_col].apply(json.dumps)

        # Saving as parquet
        transactions_df.to_parquet(
            f's3://{s3_bucket_name}/transactions/transactions_{timestamp}.parquet',
            engine='pyarrow')
        
        # Loading balance data to S3 (json)
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=f'balance/balance_{timestamp}.json',
            Body=balance,
            ContentType='application/json'
        )

        # Loading pots df to S3 bucket as parquet
        pots_df.to_parquet(
            f's3://{s3_bucket_name}/pots/pots_{timestamp}.parquet',
            engine='pyarrow'
        )
    except Exception as e:
        print(f'Error uploading extracted data to S3: {e}')
        raise

if __name__ == "__main__":
    lambda_handler(None, None)
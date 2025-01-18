"""
Monzo ETL Pipeline
"""
import os
import boto3
from datetime import datetime
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.initialise_database import initialise_database
from utils.logging_utils import Logger
from extract.extract import MonzoDataExtractor
from load.load import MonzoBronzeDataLoader
from transform.transform import transform_bronze_to_silver
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

def lambda_handler(event=None, context=None):
    try:
        # Create logs directory in lambda environment if it doesn't exist
        os.makedirs('/tmp/logs', exist_ok=True)

        # Generate a unique run ID using a timestamp
        run_id = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

        # Initialise logger
        log_file_path = os.getenv('LOCAL_LOG_PATH')
        s3_bucket = os.getenv('AWS_S3_BUCKET_NAME')
        s3_prefix = os.getenv('AWS_S3_LOG_PREFIX')
        logger_instance = Logger(log_file_path, s3_bucket, s3_prefix, logger_name='main', run_id=run_id)
        logger = logger_instance.logger

        # Create S3 client
        logger.info('[main.py] Creating S3 client')
        s3_client = boto3.client('s3')

        # Download SQLite database or create it if it doesn't exist
        local_path = os.getenv('LOCAL_DB_PATH')
        try:
            logger.info(f'[main.py] Attempting to download database from S3 to {local_path}')
            s3_client.download_file(Bucket=os.getenv('AWS_S3_BUCKET_NAME'), 
                                    Key=os.getenv('AWS_S3_DATABASE_NAME'), 
                                    Filename=local_path)
            logger.info('[main.py] Database downloaded from S3 successfully')
        except ClientError as e:
            logger.info(f'[main.py] Database not found in S3. Creating new database at {local_path}')
            initialise_database(database_path=local_path)
            logger.info('[main.py] Database created successfully')

        # Extract data
        extractor = MonzoDataExtractor(transactions_days_back=30, logger=logger)

        extracted_data = extractor.extract_data()

        # Load data into bronze layer of SQLite database
        bronze_loader = MonzoBronzeDataLoader(db_path=local_path, logger=logger)

        bronze_loader.load_data(extracted_data)

        # Transform data from bronze to silver layer
        transform_bronze_to_silver(db_path=local_path, logger=logger)

        logger.info('[main.py] Uploading database back to S3')

        # Load back to S3
        s3_client.upload_file(Filename=local_path, 
                              Bucket=os.getenv('AWS_S3_BUCKET_NAME'), 
                              Key=os.getenv('AWS_S3_DATABASE_NAME'))
        
        logger.info('[main.py] Pipeline run successfully')

        # Upload log file to S3
        logger.info('[main.py] Uploading log file to S3')
        logger_instance.upload_log_to_s3()

        return {
            'statusCode': 200,
            'body': 'ETL process completed successfully'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
    
if __name__ == "__main__":
    lambda_handler(None, None)
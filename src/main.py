"""
Monzo ETL Pipeline
"""
import os
from utils.pipeline_utils import MonzoSQLiteETL

def lambda_handler(event=None, context=None):
    try:
        # Create logs directory in lambda environment if it doesn't exist
        os.makedirs('/tmp/logs', exist_ok=True)

        etl = MonzoSQLiteETL(
            db_path='/tmp/monzo_dashboard.db', 
            log_path='/tmp/logs/monzo_etl.log', 
            s3_local_path='/tmp/monzo_dashboard.db'
        )

        etl.run_etl()

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


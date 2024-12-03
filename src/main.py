"""
Monzo ETL Pipeline
"""
from utils.pipeline_utils import MonzoSQLiteETL

def lambda_handler(event=None, context=None):
    try:
        etl = MonzoSQLiteETL(db_path='/tmp/monzo_dashboard.db', log_path='/tmp/logs/monzo_etl.log', s3_local_path='/tmp/monzo_dashboard.db')
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

# Keep this for local non-Lambda execution
if __name__ == "__main__":
    lambda_handler(None, None)

# etl = MonzoSQLiteETL(db_path='src/monzo_dashboard.db', log_path='src/logs/monzo_etl.log', s3_local_path='src/monzo_dashboard.db')
# etl.run_etl()
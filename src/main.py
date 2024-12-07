"""
Monzo ETL Pipeline
"""
from utils.pipeline_utils import MonzoSQLiteETL

if __name__=='__main__':
    etl = MonzoSQLiteETL(db_path='src/monzo_dashboard.db', log_path='src/logs/monzo_etl.log', s3_local_path='src/monzo_dashboard.db')
    etl.run_etl()
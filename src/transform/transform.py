import os
import sqlite3
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.utils import execute_sql_script

def transform_bronze_to_silver(db_path, logger):
    try:
        conn = sqlite3.connect(db_path)
        sql_dir = os.path.join(os.path.dirname(__file__), '../sql')

        execute_sql_script(conn, os.path.join(sql_dir, 'transform_bronze_to_silver.sql'))
    except Exception as e:
        logger.error(f'[transform.py] Error transforming bronze layer to silver layer: {e}')

    logger.info('[transform.py] Bronze layer successfully transformed to silver layer')
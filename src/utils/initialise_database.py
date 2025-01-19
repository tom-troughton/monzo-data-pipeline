import os
import sqlite3
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.utils import execute_sql_script

def initialise_database(database_path):
    conn = sqlite3.connect(database_path)
    sql_dir = os.path.join(os.path.dirname(__file__), '../sql')
    
    execute_sql_script(conn, os.path.join(sql_dir, 'create_bronze_layer.sql'))
    execute_sql_script(conn, os.path.join(sql_dir, 'create_silver_layer.sql'))
    execute_sql_script(conn, os.path.join(sql_dir, 'create_gold_layer.sql'))
    
    conn.close()
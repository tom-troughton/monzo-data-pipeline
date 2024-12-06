import os
import json
import boto3
import sqlite3
import logging
import shutil
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Any
from .etl_logging import S3LogHandler
try:
    from src.api.monzo_api_client import MonzoAPIClient
except ImportError:
    from api.monzo_api_client import MonzoAPIClient


class MonzoSQLiteETL:
    def __init__(
        self, 
        db_path: str,
        log_path: str = "monzo_etl.log",
        save_db_path: str | None = None,
        s3_bucket: str = 'monzo-db-bucket',
        s3_db_key: str = 'monzo_dashboard.db',
        s3_local_path: str = 'monzo_dashboard.db',
        s3_prefix: str = "monzo_logs",
        s3_upload_interval: int = 300
    ):
        """
        Initialise the ETL pipeline
        
        Args:
            db_path: Path to SQLite database file
            log_path: Path to log file
            s3_bucket: S3 bucket for log upload (optional)
            s3_prefix: Prefix for S3 logs (optional)
            s3_upload_interval: How often to upload logs in seconds (optional)
        """
        self.db_path = db_path
        self.save_db_path = save_db_path
        self.monzo_client = MonzoAPIClient()
        self.s3_bucket = s3_bucket
        self.s3_db_key = s3_db_key
        self.s3_local_path = s3_local_path
        self.s3_client = boto3.client('s3')
        
        # Set up logging
        self.setup_logging(log_path, s3_bucket, s3_prefix, s3_upload_interval)
        self.logger.info(f"Initialising MonzoSQLiteETL with database at {db_path}")
        
    def setup_logging(
        self, 
        log_path: str, 
        s3_bucket: str = None, 
        s3_prefix: str = "monzo_logs",
        s3_upload_interval: int = 300
    ):
        """
        Configure logging with file, console, and optional S3 handlers
        
        Args:
            log_path: Path where log file should be saved
            s3_bucket: S3 bucket for log upload (optional)
            s3_prefix: Prefix for S3 logs (optional)
            s3_upload_interval: How often to upload logs in seconds (optional)
        """
        self.logger = logging.getLogger('MonzoETL')
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Rotating file handler (10MB per file, keep 5 backup files)
        file_handler = RotatingFileHandler(
            log_path, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Add basic handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Add S3 handler if bucket is specified
        if s3_bucket:
            s3_handler = S3LogHandler(
                s3_client=self.s3_client,
                bucket=s3_bucket,
                prefix=s3_prefix,
                upload_interval=s3_upload_interval
            )
            s3_handler.setFormatter(formatter)
            self.logger.addHandler(s3_handler)
            self.logger.info(f"Enabled S3 log uploading to bucket: {s3_bucket}")

    def force_upload_logs(self):
        """Force immediate upload of logs to S3"""
        if self.s3_bucket:
            for handler in self.logger.handlers:
                if isinstance(handler, S3LogHandler):
                    handler.upload_logs()
                    self.logger.info("Forced upload of logs to S3 completed")
        else:
            self.logger.warning("S3 uploading not configured")

    def download_sqlite_db(self):
        """Download the SQLite database file from S3 bucket"""
        try:
            self.s3_client.download_file(self.s3_bucket, self.s3_db_key, self.s3_local_path)
            self.logger.info('Successfully downloaded database file from S3')
        except Exception as e:
            self.logger.error(f'Error downloading database file from S3: {e}')

    def upload_sqlite_db(self):
        try:
            self.s3_client.upload_file(self.s3_local_path, self.s3_bucket, self.s3_db_key)
            self.logger.info('Successfully uploaded database file from S3')
        except Exception as e:
            self.logger.error(f'Error uploading database file to S3: {e}')

    def initialise_database(self, conn):
        """Create database tables if they don't exist"""
        self.logger.info("Initialising database tables")
        try:
            cursor = conn.cursor()
            
            # Create transactions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id TEXT PRIMARY KEY,
                    description TEXT,
                    amount INTEGER NOT NULL,
                    currency TEXT NOT NULL,
                    created TIMESTAMP NOT NULL,
                    category TEXT,
                    notes TEXT,
                    is_load BOOLEAN,
                    settled TIMESTAMP,
                    local_amount INTEGER,
                    local_currency TEXT,
                    counterparty_name TEXT,
                    counterparty_account_num INTEGER,
                    counterparty_sort_code INTEGER,
                    merchant_id TEXT,
                    merchant_name TEXT,
                    merchant_category TEXT,
                    merchant_logo TEXT,
                    merchant_emoji TEXT,
                    merchant_online BOOLEAN,
                    merchant_atm BOOLEAN,
                    merchant_address TEXT,
                    merchant_city TEXT,
                    merchant_postcode TEXT,
                    merchant_country TEXT,
                    merchant_latitude REAL,
                    merchant_longitude REAL,
                    merchant_google_places_id TEXT,
                    merchant_suggested_tags TEXT,
                    merchant_foursquare_id TEXT,
                    merchant_website TEXT,
                    date_retrieved TIMESTAMP
                )
            ''')

            # Create balance table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS balance (
                    balance INTEGER,
                    total_balance INTEGER,
                    currency TEXT,
                    spend_today INTEGER,
                    date_retrieved TIMESTAMP
                )
            ''')

            # Create pots table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pots (
                    id TEXT,
                    style TEXT,
                    balance INTEGER,
                    currency TEXT,
                    type TEXT,
                    product_id TEXT,
                    current_account_id TEXT,
                    cover_image_url TEXT,
                    isa_wrapper BOOLEAN,
                    round_up BOOLEAN,
                    round_up_multiplier INTEGER,
                    is_tax_pot BOOLEAN,
                    created TIMESTAMP,
                    updated TIMESTAMP,
                    deleted BOOLEAN,
                    locked BOOLEAN,
                    available_for_bills BOOLEAN,
                    has_virtual_cards BOOLEAN,
                    date_retrieved TIMESTAMP
                )
            ''')

            conn.commit()
            self.logger.info("Database tables created successfully")
            
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {str(e)}")
            raise
    
    def extract_data(self, days_back=30):
        """
        Retrieve data from Monzo API
        
        Args:
            days_back: Number of days prior to today to get transactions for
            
        Returns:
            Transactions, balance, pots data
        """
        self.logger.info("Retrieving data from Monzo API")

        since = datetime.now() - timedelta(days=days_back)

        try:
            transactions = self.monzo_client.get_transactions(since=since)
            balance = self.monzo_client.get_balance()
            pots = self.monzo_client.list_pots()['pots']
            return transactions, balance, pots
        except Exception as e:
            self.logger.error(f"Failed to retrieve data: {str(e)}")
            raise
    
    def insert_transaction(self, transaction: Dict[str, Any], conn):
        """
        Insert a single transaction into SQLite database only if its ID doesn't already exist
    
        Args:
            transaction: Transaction data dictionary
        
        Returns:
            bool: True if inserted, False if ID already existed
        """
        try:
            cursor = conn.cursor()
            
            # First check if ID exists
            cursor.execute('''
                SELECT 1 FROM transactions WHERE id = ?
            ''', (transaction.get('id'),))
            
            if cursor.fetchone() is not None:
                self.logger.debug(f"Transaction {transaction.get('id')} already exists, skipping insertion")
                return False
            
            current_time = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO transactions (
                    id,
                    description,
                    amount,
                    currency,
                    created,
                    category,
                    notes,
                    is_load,
                    settled,
                    local_amount,
                    local_currency,
                    counterparty_name,
                    counterparty_account_num,
                    counterparty_sort_code,
                    merchant_id,
                    merchant_name,
                    merchant_category,
                    merchant_logo,
                    merchant_emoji,
                    merchant_online,
                    merchant_atm,
                    merchant_address,
                    merchant_city,
                    merchant_postcode,
                    merchant_country,
                    merchant_latitude,
                    merchant_longitude,
                    merchant_google_places_id,
                    merchant_suggested_tags,
                    merchant_foursquare_id,
                    merchant_website,
                    date_retrieved
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            ''', (
                transaction.get('id'),
                transaction.get('description'),
                transaction.get('amount'),
                transaction.get('currency'),
                transaction.get('created'),
                transaction.get('category'),
                transaction.get('notes'),
                transaction.get('is_load'),
                transaction.get('settled'),
                transaction.get('local_amount'),
                transaction.get('local_currency'),
                transaction.get('counterparty_name'),
                transaction.get('counterparty_account_num'),
                transaction.get('counterparty_sort_code'),
                transaction.get('merchant_id'),
                transaction.get('merchant_name'),
                transaction.get('merchant_category'),
                transaction.get('merchant_logo'),
                transaction.get('merchant_emoji'),
                transaction.get('merchant_online'),
                transaction.get('merchant_atm'),
                transaction.get('merchant_address'),
                transaction.get('merchant_city'),
                transaction.get('merchant_postcode'),
                transaction.get('merchant_country'),
                transaction.get('merchant_latitude'),
                transaction.get('merchant_longitude'),
                transaction.get('merchant_google_places_id'),
                json.dumps(transaction.get('merchant_suggested_tags')),
                transaction.get('merchant_foursquare_id'),
                transaction.get('merchant_website'),
                current_time
            ))
            
            self.logger.debug(f"Successfully inserted transaction {transaction.get('id')}")
            return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to insert transaction {transaction.get('id', 'unknown')}: {str(e)}")
            raise

    def insert_balance(self, balance: Dict[str, Any], conn):
        """
        Insert balance data into SQLite database
        
        Args:
            balance: Balance data dictionary from Monzo API
        """
        try:
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO balance (
                    balance,
                    total_balance,
                    currency,
                    spend_today,
                    date_retrieved
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                balance.get('balance'),
                balance.get('total_balance'),
                balance.get('currency'),
                balance.get('spend_today'),
                current_time
            ))
            
            self.logger.debug("Successfully inserted balance data")
            
        except sqlite3.Error as e:
            self.logger.error(f"Failed to insert balance data: {str(e)}")
            raise

    def insert_pot(self, pot: Dict[str, Any], conn):
        """
        Insert a single pot into SQLite database
        
        Args:
            pot: Pot data dictionary from Monzo API
        """
        try:
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO pots (
                    id,
                    style,
                    balance,
                    currency,
                    type,
                    product_id,
                    current_account_id,
                    cover_image_url,
                    isa_wrapper,
                    round_up,
                    round_up_multiplier,
                    is_tax_pot,
                    created,
                    updated,
                    deleted,
                    locked,
                    available_for_bills,
                    has_virtual_cards,
                    date_retrieved
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pot.get('id'),
                pot.get('style'),
                pot.get('balance'),
                pot.get('currency'),
                pot.get('type'),
                pot.get('product_id'),
                pot.get('current_account_id'),
                pot.get('cover_image_url'),
                pot.get('isa_wrapper', False),
                pot.get('round_up', False),
                pot.get('round_up_multiplier'),
                pot.get('is_tax_pot', False),
                pot.get('created'),
                pot.get('updated'),
                pot.get('deleted', False),
                pot.get('locked', False),
                pot.get('available_for_bills', False),
                pot.get('has_virtual_cards', False),
                current_time
            ))
            
            self.logger.debug(f"Successfully inserted pot {pot.get('id')}")
            
        except sqlite3.Error as e:
            self.logger.error(f"Failed to insert pot {pot.get('id', 'unknown')}: {str(e)}")
            raise
        
    def insert_pots(self, pots: List[Dict[str, Any]], conn):
        """
        Insert multiple pots into SQLite database
        
        Args:
            pots: List of pot data dictionaries
        """
        for pot in pots:
            self.insert_pot(pot, conn)
        self.logger.info(f"Successfully inserted {len(pots)} pots")

    def create_analytics_views(self, conn):
        """Create or replace analytics views in the database"""
        self.logger.info("Creating analytics views")
        
        try:
            cursor = conn.cursor()
            
            # Store all view definitions
            views = {
                'daily_transactions_summary': '''
                    CREATE VIEW IF NOT EXISTS daily_transactions_summary AS
                    SELECT 
                        date(created) as transaction_date,
                        COUNT(*) as num_transactions,
                        SUM(CASE WHEN amount < 0 THEN amount * -1 ELSE 0 END) / 100.0 as total_spend,
                        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) / 100.0 as total_income,
                        AVG(CASE WHEN amount < 0 THEN amount * -1 ELSE NULL END) / 100.0 as avg_transaction_value,
                        COUNT(CASE WHEN merchant_online = 1 THEN 1 END) as online_transactions,
                        COUNT(CASE WHEN merchant_atm = 1 THEN 1 END) as atm_transactions
                    FROM transactions
                    GROUP BY date(created)
                    ORDER BY transaction_date DESC
                ''',
                'category_spending_summary': '''
                    CREATE VIEW IF NOT EXISTS category_spending_summary AS
                    SELECT 
                        category,
                        COUNT(*) as transaction_count,
                        SUM(CASE WHEN amount < 0 THEN amount * -1 ELSE 0 END) / 100.0 as total_spend,
                        AVG(CASE WHEN amount < 0 THEN amount * -1 ELSE NULL END) / 100.0 as avg_transaction_value,
                        MIN(created) as first_transaction,
                        MAX(created) as last_transaction
                    FROM transactions
                    WHERE amount < 0
                    GROUP BY category
                    ORDER BY total_spend DESC
                ''',
                'merchant_spending_patterns': '''
                    CREATE VIEW IF NOT EXISTS merchant_spending_patterns AS
                    SELECT 
                        merchant_name,
                        merchant_category,
                        COUNT(*) as visit_count,
                        SUM(CASE WHEN amount < 0 THEN amount * -1 ELSE 0 END) / 100.0 as total_spend,
                        AVG(CASE WHEN amount < 0 THEN amount * -1 ELSE NULL END) / 100.0 as avg_spend_per_visit,
                        MIN(created) as first_visit,
                        MAX(created) as last_visit,
                        merchant_city,
                        merchant_country,
                        merchant_online
                    FROM transactions
                    WHERE merchant_name IS NOT NULL
                    AND amount < 0
                    GROUP BY 
                        merchant_name,
                        merchant_category,
                        merchant_city,
                        merchant_country,
                        merchant_online
                    HAVING visit_count > 1
                    ORDER BY total_spend DESC
                ''',
                'monthly_spending_trends': '''
                    CREATE VIEW IF NOT EXISTS monthly_spending_trends AS
                    SELECT 
                        strftime('%Y-%m', created) as month,
                        category,
                        COUNT(*) as transaction_count,
                        SUM(CASE WHEN amount < 0 THEN amount * -1 ELSE 0 END) / 100.0 as total_spend,
                        AVG(CASE WHEN amount < 0 THEN amount * -1 ELSE NULL END) / 100.0 as avg_transaction_value
                    FROM transactions
                    WHERE amount < 0
                    GROUP BY 
                        strftime('%Y-%m', created),
                        category
                    ORDER BY 
                        month DESC,
                        total_spend DESC
                ''',
                'location_spending': '''
                    CREATE VIEW IF NOT EXISTS location_spending AS
                    SELECT 
                        merchant_city,
                        merchant_country,
                        COUNT(*) as transaction_count,
                        COUNT(DISTINCT merchant_name) as unique_merchants,
                        SUM(CASE WHEN amount < 0 THEN amount * -1 ELSE 0 END) / 100.0 as total_spend,
                        AVG(CASE WHEN amount < 0 THEN amount * -1 ELSE NULL END) / 100.0 as avg_transaction_value
                    FROM transactions
                    WHERE merchant_city IS NOT NULL
                    AND amount < 0
                    GROUP BY 
                        merchant_city,
                        merchant_country
                    ORDER BY total_spend DESC
                ''',
                'income_sources': '''
                    CREATE VIEW IF NOT EXISTS income_sources AS
                    SELECT 
                        description,
                        counterparty_name,
                        COUNT(*) as payment_count,
                        SUM(amount) / 100.0 as total_amount,
                        AVG(amount) / 100.0 as avg_amount,
                        MIN(created) as first_payment,
                        MAX(created) as last_payment
                    FROM transactions
                    WHERE amount > 0
                    AND NOT is_load
                    GROUP BY 
                        description,
                        counterparty_name
                    ORDER BY total_amount DESC
                '''
            }
            
            # Drop existing views if they exist and recreate them
            for view_name, view_sql in views.items():
                cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                cursor.execute(view_sql)
                self.logger.info(f"Created view: {view_name}")
            
            conn.commit()
            self.logger.info("Successfully created all analytics views")
            
        except sqlite3.Error as e:
            self.logger.error(f"Failed to create analytics views: {str(e)}")
            raise

    def verify_views(self, conn):
        """Verify that all views were created successfully"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
            views = cursor.fetchall()
            view_names = [view[0] for view in views]
            self.logger.info(f"Verified views: {', '.join(view_names)}")
            return view_names
        except sqlite3.Error as e:
            self.logger.error(f"Failed to verify views: {str(e)}")
            raise

    def run_etl(self, days_back: int = 30):
        """
        Run the complete ETL process including creation of analytics views
        
        Args:
            days_back: Number of days of historical data to fetch
        """
        self.logger.info(f"Starting ETL process. Getting transactions for the previous {days_back} days")
        
        conn = None
        
        try:
            # Download sqlite db file from S3 bucket
            self.download_sqlite_db()

            # Use a single connection for all database operations
            conn = sqlite3.connect(self.db_path)
            
            # Initialize database tables
            self.initialise_database(conn)
            
            # Extract data from Monzo API
            transactions, balance, pots = self.extract_data(days_back)
            
            # Process transactions
            num_transactions = len(transactions)
            self.logger.info(f"Processing {num_transactions} transactions")
            
            for index, transaction in enumerate(transactions, 1):
                self.logger.debug(f"Processing transaction {index}/{num_transactions}")
                self.insert_transaction(transaction, conn)
            
            # Process balance
            self.logger.info("Processing balance data")
            self.insert_balance(balance, conn)
            
            # Process pots
            num_pots = len(pots)
            self.logger.info(f"Processing {num_pots} pots")
            self.insert_pots(pots, conn)
            
            # Create analytics views
            self.create_analytics_views(conn)
            
            # Verify views were created
            created_views = self.verify_views(conn)
            self.logger.info(f"Created {len(created_views)} analytics views")

            # Commit all changes
            conn.commit()
            
            # Upload sqlite db to S3 bucket
            self.upload_sqlite_db()
            
            self.logger.info("ETL process completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
            self.force_upload_logs()
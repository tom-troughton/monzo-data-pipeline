"""
Monzo ETL Pipeline
"""
import os
import time
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import json
import boto3
import sqlite3
import logging
import shutil
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Any
from api import MonzoAPIClient
import json
import boto3
import requests
from datetime import datetime, timedelta, UTC

class MonzoTokenManager:
    def __init__(self, client_id: str, client_secret: str, table_name: str):
        """
        Initialise MonzoTokenManager with Monzo credentials and DynamoDB table name.
        
        Args:
            client_id (str): Monzo API client ID
            client_secret (str): Monzo API client secret
            table_name (str): Name of the DynamoDB table for token storage
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')

    def store_tokens(self, tokens):
        """
        Store tokens in DynamoDB.
        
        Args:
            tokens (dict): Token data including access_token, refresh_token, and expires_in
        """
        table = self.dynamodb.Table(self.table_name)
        expiry = datetime.now(UTC) + timedelta(seconds=tokens['expires_in'])
        
        table.put_item(Item={
            'token_id': 'current',
            'access_token': tokens['access_token'],
            'refresh_token': tokens['refresh_token'],
            'expires_at': expiry.isoformat(),
            'updated_at': datetime.now(UTC).isoformat()
        })

    def get_stored_tokens(self):
        """Retrieve tokens from DynamoDB."""
        table = self.dynamodb.Table(self.table_name)
        response = table.get_item(Key={'token_id': 'current'})
        return response.get('Item')

    def refresh_token(self, refresh_token):
        """
        Get new access token using refresh token.
        
        Args:
            refresh_token (str): The refresh token to use
        
        Returns:
            dict: New token data
        """
        response = requests.post(
            'https://api.monzo.com/oauth2/token',
            data={
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': refresh_token
            }
        )
        return response.json()

    def get_valid_token(self):
        """
        Get a valid Monzo access token, refreshing if necessary.
        
        Returns:
            dict: Response containing either a valid access token or an error
        """
        stored_tokens = self.get_stored_tokens()
        
        if not stored_tokens:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No valid tokens found. Initial authentication required.'
                })
            }

        expires_at = datetime.fromisoformat(stored_tokens['expires_at'])
        
        if expires_at - datetime.now(UTC) < timedelta(minutes=5):
            new_tokens = self.refresh_token(stored_tokens['refresh_token'])
            self.store_tokens(new_tokens)
            return {
                'statusCode': 200,
                'body': json.dumps({'access_token': new_tokens['access_token']})
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({'access_token': stored_tokens['access_token']})
        }

class MonzoAPIClient:
    """
    A client for interacting with the Monzo API.

    Credentials retrieved via AWS secrets manager.
    """
    def __init__(self):
        self.base_url = 'https://api.monzo.com'
        self.monzo_credentials = get_secret('monzo-api-credentials')
        self.token_manager = MonzoTokenManager(
            client_id=self.monzo_credentials['monzo_client_id'],
            client_secret=self.monzo_credentials['monzo_client_secret'],
            table_name='monzo-tokens'
        )
        self.access_token = json.loads(self.token_manager.get_valid_token()['body'])['access_token']
        self.account_id = self.monzo_credentials['monzo_account_id']
        self.headers = {'Authorization': f'Bearer {self.access_token}',
                        'Content-Type': 'application/json'}
        
        if not self.access_token:
            raise ValueError("Access token is required. Complete the OAuth flow first.")
        
        if not self.account_id:
            raise ValueError("Account ID is required. You can find this in your Monzo account settings.")

    def _extract_merchant_info(self, transactions_data):
        """
        Extract merchant information from nested transaction data and flatten it
        Returns a list of transactions with merchant info flattened into the main dict
        """
        processed_transactions = []
        
        for transaction in transactions_data.get('transactions', []):
            # Create a new dict with basic transaction info
            processed_tx = {
                'id': transaction.get('id'),
                'description': transaction.get('description'),
                'amount': transaction.get('amount', 0),
                'currency': transaction.get('currency'),
                'created': transaction.get('created'),
                'category': transaction.get('category'),
                'notes': transaction.get('notes'),
                'is_load': transaction.get('is_load', False),
                'settled': transaction.get('settled'),
                'local_amount': transaction.get('local_amount', 0),
                'local_currency': transaction.get('local_currency'),
                'counterparty_name': json.loads(json.dumps(transaction.get('counterparty'))).get('name'),
                'counterparty_account_num': json.loads(json.dumps(transaction.get('counterparty'))).get('account_number'),
                'counterparty_sort_code': json.loads(json.dumps(transaction.get('counterparty'))).get('sort_code')
            }
            
            # Initialise all merchant fields with None
            merchant_fields = {
                'merchant_id': None,
                'merchant_name': None,
                'merchant_category': None,
                'merchant_logo': None,
                'merchant_emoji': None,
                'merchant_online': False,
                'merchant_atm': False,
                'merchant_address': None,
                'merchant_city': None,
                'merchant_postcode': None,
                'merchant_country': None,
                'merchant_latitude': None,
                'merchant_longitude': None,
                'merchant_google_places_id': None,
                'merchant_suggested_tags': None,
                'merchant_foursquare_id': None,
                'merchant_website': None
            }
            
            # Extract merchant information if it exists
            merchant = transaction.get('merchant')
            if merchant:
                # Update merchant details
                merchant_fields.update({
                    'merchant_id': merchant.get('id'),
                    'merchant_name': merchant.get('name'),
                    'merchant_category': merchant.get('category'),
                    'merchant_logo': merchant.get('logo'),
                    'merchant_emoji': merchant.get('emoji'),
                    'merchant_online': merchant.get('online', False),
                    'merchant_atm': merchant.get('atm', False)
                })
                
                # Extract address if it exists
                address = merchant.get('address', {})
                if address:
                    merchant_fields.update({
                        'merchant_address': address.get('address'),
                        'merchant_city': address.get('city'),
                        'merchant_postcode': address.get('postcode'),
                        'merchant_country': address.get('country'),
                        'merchant_latitude': address.get('latitude'),
                        'merchant_longitude': address.get('longitude')
                    })
            
            # Add all merchant fields to the transaction
            processed_tx.update(merchant_fields)
            processed_transactions.append(processed_tx)
        
        return processed_transactions

    def whoami(self):
        """
        Call the /ping/whoami endpoint to verify authentication and get user information
        """
        response = requests.get(
            f'{self.base_url}/ping/whoami', 
            headers=self.headers
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def list_accounts(self):
        response = requests.get(
            f'{self.base_url}/accounts', 
            headers=self.headers
        )
        
        if response.status_code == 200:
            accounts = response.json().get('accounts', [])
            for account in accounts:
                print(f"Account ID: {account['id']}, Type: {account['type']}")
        else:
            response.raise_for_status()

    def list_pots(self):
        """
        Retrieve a list of pots associated with the account
        """
        params = {
            'current_account_id': self.account_id
        }
        
        response = requests.get(
            f'{self.base_url}/pots', 
            headers=self.headers,
            params=params
        )

        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_transactions(self, 
                          limit=200, 
                          since=None, 
                          before=None):
        """
        Retrieve transactions with optional filtering
        
        Args:
            limit: Maximum number of transactions to retrieve (default 100)
            since: Retrieve transactions since this date
            before: Retrieve transactions before this date
        """
        # Prepare query parameters
        params = {
            'account_id': self.account_id,
            'limit': limit,
            'expand[]': 'merchant'
        }
        
        # Add optional date filters
        if since:
            # Ensure the date is in ISO 8601 format with UTC timezone
            if isinstance(since, datetime):
                since = since.isoformat() + 'Z'
            params['since'] = since
        
        if before:
            # Ensure the date is in ISO 8601 format with UTC timezone
            if isinstance(before, datetime):
                before = before.isoformat() + 'Z'
            params['before'] = before
        
        # Make the API call
        response = requests.get(
            f'{self.base_url}/transactions', 
            headers=self.headers,
            params=params
        )
        
        # Check for successful response
        if response.status_code == 200:
            return self._extract_merchant_info(response.json())
        else:
            # Handle potential errors
            response.raise_for_status()
    
    def get_balance(self):
        """
        Retrieve current balance and spending information
        """
        response = requests.get(
            f'{self.base_url}/balance',
            headers=self.headers,
            params={'account_id': self.account_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            balance = {
                'balance': data['balance'],
                'total_balance': data['total_balance'],
                'currency': data['currency'],
                'spend_today': abs(data['spend_today'])
            }
            return balance
        else:
            response.raise_for_status()

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

class S3LogHandler(logging.Handler):
    """Custom logging handler that uploads logs to S3"""
    
    def __init__(self, s3_client, bucket: str, prefix: str, upload_interval: int = 300):
        """
        Initialise S3 log handler
        
        Args:
            bucket: S3 bucket name
            prefix: Prefix for S3 keys (folder structure)
            upload_interval: How often to upload logs (in seconds)
        """
        super().__init__()
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.upload_interval = upload_interval
        self.buffer = []
        self.last_upload = time.time()

    def emit(self, record):
        """Process a log record"""
        msg = self.format(record)
        self.buffer.append(msg)
        
        # Upload if enough time has passed
        if time.time() - self.last_upload > self.upload_interval:
            self.upload_logs()

    def upload_logs(self):
        """Upload accumulated logs to S3"""
        if not self.buffer:
            return
            
        # Create log content
        log_content = '\n'.join(self.buffer)
        
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        s3_key = f"{self.prefix}/monzo_etl_{timestamp}.log"
        
        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=log_content
            )
            
            # Clear buffer and update upload time
            self.buffer = []
            self.last_upload = time.time()
            
        except ClientError as e:
            # Keep logs in buffer if upload fails
            print(f"Failed to upload logs to S3: {str(e)}")

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

    def initialise_database(self):
        """Create database tables if they don't exist"""
        self.logger.info("Initialising database tables")
        try:
            with sqlite3.connect(self.db_path) as conn:
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
    
    def insert_transaction(self, transaction: Dict[str, Any]):
        """
        Insert a single transaction into SQLite database only if its ID doesn't already exist
    
        Args:
            transaction: Transaction data dictionary
        
        Returns:
            bool: True if inserted, False if ID already existed
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
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

    def insert_balance(self, balance: Dict[str, Any]):
        """
        Insert balance data into SQLite database
        
        Args:
            balance: Balance data dictionary from Monzo API
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
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

    def insert_pot(self, pot: Dict[str, Any]):
        """
        Insert a single pot into SQLite database
        
        Args:
            pot: Pot data dictionary from Monzo API
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
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
        
    def insert_pots(self, pots: List[Dict[str, Any]]):
        """
        Insert multiple pots into SQLite database
        
        Args:
            pots: List of pot data dictionaries
        """
        for pot in pots:
            self.insert_pot(pot)
        self.logger.info(f"Successfully inserted {len(pots)} pots")

    def run_etl(self, days_back: int = 30):
        """
        Run the complete ETL process
        
        Args:
            days_back: Number of days of historical data to fetch
        """
        self.logger.info(f"Starting ETL process. Getting transactions for the previous {days_back} days")
        
        try:
            # Download sqlite db file from S3 bucket
            self.download_sqlite_db()

            # Initialise database if needed
            self.initialise_database()
            
            # Extract data from Monzo API
            transactions, balance, pots = self.extract_data(days_back)
            
            # Process transactions
            num_transactions = len(transactions)
            self.logger.info(f"Processing {num_transactions} transactions")
            
            for index, transaction in enumerate(transactions, 1):
                self.logger.debug(f"Processing transaction {index}/{num_transactions}")
                self.insert_transaction(transaction)
            
            # Process balance
            self.logger.info("Processing balance data")
            self.insert_balance(balance)
            
            # Process pots
            num_pots = len(pots)
            self.logger.info(f"Processing {num_pots} pots")
            self.insert_pots(pots)

            # Upload sqlite db to S3 bucket
            self.upload_sqlite_db()
            
            self.logger.info("ETL process completed successfully")
            
            # Force upload logs if S3 is configured
            self.force_upload_logs()

            if self.save_db_path:
                # Create timestamp and new filename
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                save_filename = f'monzo_db_{timestamp}.db'
                save_path = os.path.join(self.save_db_path, save_filename)
                
                # Ensure directory exists
                os.makedirs(self.save_db_path, exist_ok=True)
                
                # Copy the database file to the save location
                shutil.copy2(self.db_path, save_path)
                
            # Delete the temporary database file
            os.remove(self.db_path)
                
        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
            raise


if __name__ == "__main__":
    etl = MonzoSQLiteETL(db_path='src/monzo_dashboard.db', log_path='src/logs/monzo_etl.log', s3_local_path='src/monzo_dashboard.db')
    etl.run_etl()

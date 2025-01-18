import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Any

class MonzoBronzeDataLoader:
    """
    Retrieves sqlite database from S3 bucket and loads data into it then reuploads to S3 bucket

    Args:
        db_path: Path to SQLite database file
        s3_bucket: S3 bucket for log upload (optional)
        s3_prefix: Prefix for S3 logs (optional)
    """
    def __init__(
            self, 
            db_path: str,
            logger = None
    ):
        self.db_path = db_path
        self.logger = logger

        # self.logger.info(f"Initialising MonzoDataLoader with database at {db_path}")

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
            
            # Check if ID exists
            cursor.execute('''
                SELECT 1 FROM bronze_transactions WHERE id = ?
            ''', (transaction.get('id'),))
            
            if cursor.fetchone() is not None:
                self.logger.debug(f"[load.py] Transaction {transaction.get('id')} already exists, skipping insertion")
                return False
            
            current_time = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO bronze_transactions (
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
            
            self.logger.debug(f"[load.py] Successfully inserted transaction {transaction.get('id')}")
            return True
                
        except sqlite3.Error as e:
            self.logger.error(f"[load.py] Failed to insert transaction {transaction.get('id', 'unknown')}: {str(e)}")
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
                INSERT INTO bronze_balance (
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
            
            self.logger.debug("[load.py] Successfully inserted balance data")
            
        except sqlite3.Error as e:
            self.logger.error(f"[load.py] Failed to insert balance data: {str(e)}")
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
                INSERT INTO bronze_pots (
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
            
            self.logger.debug(f"[load.py] Successfully inserted pot {pot.get('id')}")
            
        except sqlite3.Error as e:
            self.logger.error(f"[load.py] Failed to insert pot {pot.get('id', 'unknown')}: {str(e)}")
            raise

    def insert_pots(self, pots: List[Dict[str, Any]], conn):
        """
        Insert multiple pots into SQLite database
        
        Args:
            pots: List of pot data dictionaries
        """
        for pot in pots.get('pots'):
            self.insert_pot(pot, conn)
        self.logger.info(f"[load.py] Successfully inserted {len(pots)} pots")

    def load_data(self, data):
        """
        Load data into SQLite database
        
        Args:
            data: Data dictionary containing transactions, balance, and pots
        """
        self.logger.info("[load.py] Loading data into SQLite database")

        conn = None

        try:
            # Use a single connection for all database operations
            conn = sqlite3.connect(self.db_path)

            transactions_data = data.get('transactions')
            balance_data = data.get('balance')
            pots_data = data.get('pots')

            # Insert transactions
            self.logger.info(f"[load.py] Loading {len(transactions_data)} transactions")
            for index, transaction in enumerate(transactions_data, 1):
                self.logger.debug(f"[load.py] Processing transaction {index}/{len(transactions_data)}")
                self.insert_transaction(transaction, conn)
            
            # Insert balance
            self.logger.info("[load.py] Loading balance data")
            self.insert_balance(balance_data, conn)
            
            # Insert pots
            self.logger.info(f"[load.py] Loading pots data")
            self.insert_pots(pots_data, conn)
            
            # Commit all changes
            conn.commit()
            
            self.logger.info("[load.py] Loading completed successfully")
        except Exception as e:
            self.logger.error(f"[load.py] Loading failed: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
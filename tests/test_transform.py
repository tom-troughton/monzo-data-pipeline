import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import pytest
import sqlite3
from src.transform.transform import transform_bronze_to_silver

@pytest.fixture
def mock_logger():
    import logging
    logger = logging.getLogger('test_logger')
    logger.addHandler(logging.NullHandler())
    return logger

@pytest.fixture
def db_connection(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()

def test_transform_bronze_to_silver(mock_logger, db_connection):
    # Create bronze_transactions table and insert sample data
    cursor = db_connection.cursor()
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS bronze_transactions (
            id TEXT PRIMARY KEY,
            description TEXT,
            amount INTEGER,
            currency TEXT,
            created TIMESTAMP,
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
            merchant_website TEXT
        );
        INSERT INTO bronze_transactions (id, description, amount, currency, created, category, notes, is_load, settled, local_amount, local_currency, counterparty_name, counterparty_account_num, counterparty_sort_code, merchant_id, merchant_name, merchant_category, merchant_logo, merchant_emoji, merchant_online, merchant_atm, merchant_address, merchant_city, merchant_postcode, merchant_country, merchant_latitude, merchant_longitude, merchant_google_places_id, merchant_suggested_tags, merchant_foursquare_id, merchant_website)
        VALUES ('tx_0001', 'Test transaction', 100, 'GBP', '2025-01-01T00:00:00Z', 'general', '', 0, '2025-01-02T00:00:00Z', 100, 'GBP', 'Test Counterparty', 12345678, 123456, 'merch_0001', 'Test Merchant', 'general', '', '', 0, 0, '', 'Test City', 'TC1 1TC', 'GBR', 51.509865, -0.118092, '', '', '', '');
    ''')
    db_connection.commit()

    transform_bronze_to_silver(db_path=db_connection, logger=mock_logger)
    
    cursor.execute("SELECT * FROM silver_transactions WHERE id = 'tx_0001'")
    result = cursor.fetchone()
    
    assert result is not None
    assert result[0] == 'tx_0001'
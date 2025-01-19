import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import pytest
import sqlite3
from src.load.load import MonzoBronzeDataLoader

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

def test_load_data(mock_logger, db_connection):
    loader = MonzoBronzeDataLoader(db_path=db_connection, logger=mock_logger)
    sample_data = {
        'transactions': [
            {
                'id': 'tx_0001',
                'description': 'Test transaction',
                'amount': 100,
                'currency': 'GBP',
                'created': '2025-01-01T00:00:00Z',
                'category': 'general',
                'notes': '',
                'is_load': False,
                'settled': '2025-01-02T00:00:00Z',
                'local_amount': 100,
                'local_currency': 'GBP',
                'counterparty_name': 'Test Counterparty',
                'counterparty_account_num': '12345678',
                'counterparty_sort_code': '12-34-56',
                'merchant_id': 'merch_0001',
                'merchant_name': 'Test Merchant',
                'merchant_category': 'general',
                'merchant_logo': '',
                'merchant_emoji': '',
                'merchant_online': False,
                'merchant_atm': False,
                'merchant_address': '',
                'merchant_city': 'Test City',
                'merchant_postcode': 'TC1 1TC',
                'merchant_country': 'GBR',
                'merchant_latitude': 50.409865,
                'merchant_longitude': -0.118092,
                'merchant_google_places_id': '',
                'merchant_suggested_tags': '',
                'merchant_foursquare_id': '',
                'merchant_website': ''
            }
        ]
    }
    loader.load_data(sample_data)
    
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM bronze_transactions WHERE id = 'tx_0001'")
    result = cursor.fetchone()
    
    assert result is not None
    assert result[0] == 'tx_0001'
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import pytest
from src.extract.extract import MonzoDataExtractor

@pytest.fixture
def mock_logger():
    import logging
    logger = logging.getLogger('test_logger')
    logger.addHandler(logging.NullHandler())
    return logger

def test_extract_data(mock_logger):
    extractor = MonzoDataExtractor(transactions_days_back=30, logger=mock_logger)
    data = extractor.extract_data()
    
    assert data is not None
    assert 'transactions' in data
    assert isinstance(data['transactions'], list)
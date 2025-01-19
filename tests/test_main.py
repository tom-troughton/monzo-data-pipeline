import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import pytest
from unittest.mock import patch
from main import lambda_handler

@pytest.fixture
def mock_logger():
    import logging
    logger = logging.getLogger('test_logger')
    logger.addHandler(logging.NullHandler())
    return logger

@patch('main.MonzoDataExtractor.extract_data')
@patch('main.MonzoBronzeDataLoader.load_data')
@patch('main.transform_bronze_to_silver')
@patch('main.boto3.client')
def test_lambda_handler(mock_boto3_client, mock_transform, mock_load_data, mock_extract_data, mock_logger):
    mock_extract_data.return_value = {'transactions': []}
    mock_load_data.return_value = None
    mock_transform.return_value = None
    mock_boto3_client.return_value.upload_file.return_value = None

    response = lambda_handler(event=None, context=None)
    
    assert response['statusCode'] == 200
    assert response['body'] == 'ETL process completed successfully'
import unittest
from utils.api.api_client import MonzoAPIClient

class TestAPIClient(unittest.TestCase):
    def __init__(self):
        api_client = MonzoAPIClient()

    def test_authenticated(self):
        self.assertEqual(self.api_client.whoami()['Authenticated'], True)
    
    def test_transactions_received(self):
        self.assertGreater(len(self.api_client.get_transactions()), 1)
import sys
import os
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.api import MonzoAPIClient

class MonzoDataExtractor:
    """Extract data from Monzo API using MonzoAPIClient and MonzoTokenManager to refresh access token"""
    def __init__(self, transactions_days_back: int = 30, logger=None):
        self.logger = logger
        self.monzo_client = MonzoAPIClient()
        self.transactions_days_back = transactions_days_back

    def extract_data(self):
        self.logger.info("[extract.py] Extracting data from Monzo API")

        try:
            since = datetime.now() - timedelta(days=self.transactions_days_back)
            transactions_data = self.monzo_client.get_transactions(since=since)
            balance_data = self.monzo_client.get_balance()
            pots_data = self.monzo_client.list_pots()
            self.logger.info(f"[extract.py] Data extracted from Monzo API successfully ({len(transactions_data)} transactions + balance + pots data)")
            return {
                'transactions': transactions_data,
                'balance': balance_data,
                'pots': pots_data
            }
        except Exception as e:
            self.logger.error(f"[extract.py] Error extracting data from Monzo API: {e}")
            raise
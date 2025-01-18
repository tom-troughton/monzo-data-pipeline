import json
import requests
from datetime import datetime
from src.utils import get_secret
from .token_manager import MonzoTokenManager

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
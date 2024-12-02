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
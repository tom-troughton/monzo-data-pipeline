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
        
        # Validate credentials
        if not all([client_id, client_secret, table_name]):
            raise ValueError("Missing required credentials: client_id, client_secret, and table_name are required")

    def store_tokens(self, tokens):
        """
        Store tokens in DynamoDB.
        
        Args:
            tokens (dict): Token data including access_token, refresh_token, and expires_in
        """
        if not tokens.get('access_token'):
            raise ValueError("Invalid tokens: access_token is required")
            
        table = self.dynamodb.Table(self.table_name)
        expires_in = tokens.get('expires_in', 14400)  
        expiry = datetime.now(UTC) + timedelta(seconds=expires_in)
        
        try:
            table.put_item(Item={
                'token_id': 'current',
                'access_token': tokens['access_token'],
                'refresh_token': tokens['refresh_token'],
                'expires_at': expiry.isoformat(),
                'updated_at': datetime.now(UTC).isoformat()
            })
        except Exception as e:
            raise Exception(f"Failed to store tokens in DynamoDB: {str(e)}")

    def get_stored_tokens(self):
        """
        Retrieve tokens from DynamoDB.
        
        Returns:
            dict: Token data or None if not found
        """
        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.get_item(Key={'token_id': 'current'})
            return response.get('Item')
        except Exception as e:
            raise Exception(f"Failed to retrieve tokens from DynamoDB: {str(e)}")

    def refresh_token(self, refresh_token):
        """
        Get new access token using refresh token and update stored refresh token.
        
        Args:
            refresh_token (str): The refresh token to use
        
        Returns:
            dict: New token data
        """
        if not refresh_token:
            raise ValueError("refresh_token is required")
            
        try:
            response = requests.post(
                'https://api.monzo.com/oauth2/token',
                data={
                    'grant_type': 'refresh_token',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'refresh_token': refresh_token
                }
            )
            
            if response.status_code != 200:
                error_data = response.json()
                if error_data.get('code') == 'unauthorized.bad_refresh_token.evicted':
                    raise Exception(
                        "Authentication required: Your session has expired due to a login elsewhere. "
                        "Please complete the OAuth flow again to obtain new tokens."
                    )
                raise Exception(f"Token refresh failed with status {response.status_code}: {response.text}")
            
            new_tokens = response.json()
            
            # If refresh_token is not in response, use the existing one
            if 'refresh_token' not in new_tokens:
                new_tokens['refresh_token'] = refresh_token
            
            try:
                # Update the refresh token in AWS Secrets Manager
                secrets_manager = boto3.client('secretsmanager')
                secret_response = secrets_manager.get_secret_value(SecretId='monzo-api-credentials')
                credentials = json.loads(secret_response['SecretString'])
                credentials['monzo_refresh_token'] = new_tokens['refresh_token']
                
                secrets_manager.put_secret_value(
                    SecretId='monzo-api-credentials',
                    SecretString=json.dumps(credentials)
                )
            except Exception as e:
                raise Exception(f"Failed to update refresh token in Secrets Manager: {str(e)}")
            
            return new_tokens
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"HTTP request failed during token refresh: {str(e)}")

    def get_valid_token(self):
        """
        Get a valid Monzo access token, refreshing if necessary.
        
        Returns:
            dict: Response containing either a valid access token or an error
        """
        try:
            stored_tokens = self.get_stored_tokens()
            
            if not stored_tokens:
                return {
                    'statusCode': 401,
                    'body': json.dumps({
                        'error': 'No valid tokens found. Initial authentication required.'
                    })
                }

            try:
                # Get the up-to-date refresh token from AWS Secrets Manager
                secrets_manager = boto3.client('secretsmanager')
                secret_response = secrets_manager.get_secret_value(SecretId='monzo-api-credentials')
                credentials = json.loads(secret_response['SecretString'])
                current_refresh_token = credentials.get('monzo_refresh_token')
                
                if not current_refresh_token:
                    raise Exception("No refresh token found in Secrets Manager")

                new_tokens = self.refresh_token(current_refresh_token)
                self.store_tokens(new_tokens)
                return {
                    'statusCode': 200,
                    'body': json.dumps({'access_token': new_tokens['access_token']})
                }
            except Exception as e:
                return {
                    'statusCode': 401,
                    'body': json.dumps({
                        'error': str(e),
                        'requires_reauth': True
                    })
                }
                
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f"Token validation failed: {str(e)}"
                })
            }
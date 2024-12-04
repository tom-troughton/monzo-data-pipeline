from http.server import HTTPServer, BaseHTTPRequestHandler
from monzo_api_client import get_secret
import webbrowser
import urllib.parse
import requests

class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/callback'):
            # Extract authorization code from callback URL
            query_components = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            auth_code = query_components['code'][0]
            
            # Exchange authorization code for tokens
            token_response = requests.post(
                'https://api.monzo.com/oauth2/token',
                data={
                    'grant_type': 'authorization_code',
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'redirect_uri': 'http://localhost:8000/callback',
                    'code': auth_code
                }
            )
            
            # Store the tokens
            tokens = token_response.json()
            print("\nAccess Token:", tokens['access_token'])
            print("\nRefresh Token:", tokens['refresh_token'])
            
            # Send response to browser
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Authorization successful! You can close this window.")
            
            # Stop the server
            raise KeyboardInterrupt


secrets = get_secret('monzo-api-credentials')
CLIENT_ID = secrets['monzo_client_id']
CLIENT_SECRET = secrets['monzo_client_secret']

# Generate authorisation URL
auth_url = (
    f"https://auth.monzo.com/?client_id={CLIENT_ID}"
    f"&redirect_uri=http://localhost:8000/callback"
    f"&response_type=code"
    f"&state=random_state_string"
)

# Start local server
server = HTTPServer(('localhost', 8000), OAuthHandler)
print(f"Opening browser for authorisation...")
webbrowser.open(auth_url)

try:
    server.serve_forever()
except KeyboardInterrupt:
    server.server_close()
    print("\nAuthorisation flow completed!")
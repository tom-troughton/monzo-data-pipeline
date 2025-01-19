import boto3
import json

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def update_secret(secret_name, new_secret_value):
    """Update secret in AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.put_secret_value(
        SecretId=secret_name,
        SecretString=json.dumps(new_secret_value)
    )
    return response

def execute_sql_script(conn, script_path):
    with open(script_path, 'r') as file:
        sql_script = file.read()
    cursor = conn.cursor()
    cursor.executescript(sql_script)
    conn.commit()
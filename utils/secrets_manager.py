import os
from datetime import datetime, timedelta
import boto3
import json 
import pytz
import re
from botocore.exceptions import ClientError

def get_credentials(secret_name: str):
    client = boto3.client(
        service_name='secretsmanager',
        region_name='ap-south-1'
    )
    try:
        secret_id = f"{secret_name}"
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
        print("parameters %s", get_secret_value_response)
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])

            return secret
        else:
            raise ValueError("SecretString not found in response")
    except Exception as e:
        print(f"Error getting secret: {e}")
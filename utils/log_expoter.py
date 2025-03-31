import gzip
import json
import boto3
import base64
import os
from datetime import datetime
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'my-logs-bucket-aws-lambda')

def lambda_handler(event, context):
    print(f"Received log export event: {json.dumps(event)}")
    
    # CloudWatch Logs sends data in compressed format (gzip)
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    
    log_data = json.loads(uncompressed_payload)
    
    # Generate a unique file name based on timestamp and log group
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    log_group = log_data.get('logGroup', 'unknown').replace('/', '-')
    log_stream = log_data.get('logStream', 'unknown')
    
    # Construct S3 object key with a more organized structure
    s3_key = f"logs/{log_group}/{log_stream}/{timestamp}.json"
    
    # Write the uncompressed log data to S3
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(log_data),
            ContentType='application/json'
        )
        print(f"Successfully forwarded log data to s3://{BUCKET_NAME}/{s3_key}")
        
        # Log some useful metrics
        log_events_count = len(log_data.get('logEvents', []))
        print(f"Exported {log_events_count} log events from {log_group}/{log_stream}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'bucket': BUCKET_NAME,
                'key': s3_key,
                'eventsCount': log_events_count
            })
        }
    except Exception as e:
        print(f"Error uploading logs to S3: {e}")
        raise e
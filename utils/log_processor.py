import json
import gzip
import base64
import os
import requests
from io import BytesIO
import time

def lambda_handler(event, context):
    """
    Process CloudWatch Logs with metrics and forward to Pushgateway
    """
    print(f"Received log processor event: {json.dumps(event)}")
    
    # Get Pushgateway URL from environment variables
    pushgateway_url = os.environ.get('PUSHGATEWAY_URL')
    if not pushgateway_url:
        print("PUSHGATEWAY_URL environment variable not set. Skipping metrics forwarding.")
        return {
            'statusCode': 400,
            'body': 'PUSHGATEWAY_URL not configured'
        }
    
    # Process CloudWatch Logs event
    if 'awslogs' in event:
        # Decode and decompress the log data
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        uncompressed_payload = gzip.decompress(compressed_payload)
        log_events = json.loads(uncompressed_payload)
        
        print(f"Decoded log events: {json.dumps(log_events)}")
        
        # Process each log event
        metrics_count = 0
        for log_event in log_events.get('logEvents', []):
            try:
                # Print the raw message for debugging
                raw_message = log_event.get('message', '')
                print(f"Raw log message: {raw_message}")
                
                # Check if the message starts with "INFO", "ERROR", etc. and extract JSON
                if ' {' in raw_message:
                    # Split at the first occurrence of a space followed by '{'
                    json_str = raw_message[raw_message.find(' {'):]
                    log_message = json.loads(json_str)
                else:
                    # Try standard JSON parse
                    log_message = json.loads(raw_message)
                
                print(f"Parsed log message: {json.dumps(log_message)}")
                
                # Check if this is a metrics message
                if log_message.get('message_type') == 'lambda_metrics':
                    metrics_count += 1
                    metric_data = log_message.get('metric_data', {})
                    
                    # Push to Prometheus Pushgateway
                    push_metrics_to_gateway(metric_data, pushgateway_url)
                    
            except Exception as e:
                print(f"Error processing log event: {str(e)}")
                print(f"Error details: {repr(e)}")
                print(f"Log event was: {json.dumps(log_event)}")
        
        print(f"Processed {metrics_count} metrics from {len(log_events.get('logEvents', []))} log events")
    else:
        print("No 'awslogs' data found in event")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Log processing complete')
    }

def push_metrics_to_gateway(metric_data, pushgateway_url):
    """Format and push metrics to Prometheus Pushgateway"""
    try:
        # Format the data for Prometheus
        prometheus_data = ""
        function_name = metric_data['function_name'].replace('-', '_')
        endpoint = metric_data['endpoint'].replace('/', '_')
        
        # Add timestamp to make each push unique
        timestamp = int(time.time())
        
        # Format metric data based on type
        if metric_data.get('metric_type') == 'invocation':
            # Invocation counter with timestamp in the label
            prometheus_data += f"""
# TYPE lambda_invocation_event gauge
lambda_invocation_event{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",status="{metric_data['status']}",environment="{metric_data['environment']}",timestamp="{timestamp}"}} 1

# TYPE lambda_duration_seconds gauge
lambda_duration_seconds{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",environment="{metric_data['environment']}"}} {metric_data['duration_seconds']}
"""
            # Add memory usage if available
            if metric_data.get('memory_used_mb'):
                prometheus_data += f"""
# TYPE lambda_memory_used_mb gauge
lambda_memory_used_mb{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",environment="{metric_data['environment']}"}} {metric_data['memory_used_mb']}
"""

            # Add cold start if this was one
            if metric_data.get('cold_start'):
                prometheus_data += f"""
# TYPE lambda_cold_start_event gauge
lambda_cold_start_event{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",environment="{metric_data['environment']}",timestamp="{timestamp}"}} 1
"""
        
        elif metric_data.get('metric_type') == 'error':
            prometheus_data += f"""
# TYPE lambda_error_event gauge
lambda_error_event{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",error_type="{metric_data['error_type']}",environment="{metric_data['environment']}",timestamp="{timestamp}"}} 1
"""
        
        # Push to Pushgateway
        url = f"{pushgateway_url}/metrics/job/lambda_{function_name}/instance/{endpoint}"
        print(f"Sending metrics to: {url}")
        print(f"Prometheus data: {prometheus_data}")
        
        response = requests.post(url, data=prometheus_data)
        response.raise_for_status()
        print(f"Successfully pushed metrics to Pushgateway: {url} with status code {response.status_code}")
        
    except Exception as e:
        print(f"Failed to push metrics to Pushgateway: {str(e)}")
        print(f"Full error: {repr(e)}")
        print(f"Metric data that failed: {json.dumps(metric_data)}")
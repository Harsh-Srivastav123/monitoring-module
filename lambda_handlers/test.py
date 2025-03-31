import json
import logging
import time
import uuid
import traceback
from datetime import datetime
from utils.observability import lambda_observability_decorator


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def setup_structured_logger():
    """Configure logger for structured logging"""
    # Remove default handlers and add our own
    for handler in logger.handlers:
        logger.removeHandler(handler)
    
    # Create a handler that will work well with CloudWatch
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def log_with_context(level, message, **context):
    """Log with additional context in JSON format"""
    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "message": message,
        **context
    }
    
    if level == "INFO":
        logger.info(json.dumps(log_data))
    elif level == "WARNING":
        logger.warning(json.dumps(log_data))
    elif level == "ERROR":
        logger.error(json.dumps(log_data))
    elif level == "DEBUG":
        logger.debug(json.dumps(log_data))

def process_records(records):
    """Process some sample records and log the activity"""
    processed = 0
    errors = 0
    start_time = time.time()
    
    log_with_context("INFO", "Starting record processing", 
                     record_count=len(records), 
                     process_id=str(uuid.uuid4()))
    
    for i, record in enumerate(records):
        try:
            # Simulate processing time
            time.sleep(0.1)
            
            # Log progress for long-running operations
            if (i + 1) % 10 == 0:
                log_with_context("INFO", "Processing progress", 
                               processed=i+1, 
                               total=len(records), 
                               elapsed_sec=round(time.time() - start_time, 2))
            
            # Simulate a business logic error for some records
            if record.get('status') == 'invalid':
                raise ValueError(f"Invalid record format: {record.get('id')}")
                
            # More business logic here...
            processed += 1
            
        except Exception as e:
            errors += 1
            log_with_context("ERROR", f"Failed to process record", 
                           record_id=record.get('id', 'unknown'),
                           error=str(e),
                           traceback=traceback.format_exc())
    
    # Log completion with performance metrics
    duration = time.time() - start_time
    log_with_context("INFO", "Completed record processing",
                    processed_count=processed,
                    error_count=errors,
                    duration_sec=round(duration, 2),
                    throughput=round(processed/duration, 2) if duration > 0 else 0)
    
    return {
        "processed": processed,
        "errors": errors,
        "duration_sec": round(duration, 2)
    }

@lambda_observability_decorator('/test')
@inject_logger_context
@log_lambda_invocation()
def main(event, context):
    """Main Lambda handler function"""
    setup_structured_logger()
    request_id = context.aws_request_id if hasattr(context, 'aws_request_id') else str(uuid.uuid4())
    
    log_with_context("INFO", "Lambda function invoked", 
                    request_id=request_id,
                    function_name=context.function_name if hasattr(context, 'function_name') else 'unknown',
                    memory_limit_mb=context.memory_limit_in_mb if hasattr(context, 'memory_limit_in_mb') else 'unknown',
                    remaining_time_ms=context.get_remaining_time_in_millis() if hasattr(context, 'get_remaining_time_in_millis') else 'unknown')
    
    try:
        # Parse the incoming event
        body = {}
        if event.get('body'):
            try:
                body = json.loads(event['body'])
                log_with_context("INFO", "Parsed request body", size_bytes=len(event['body']))
            except json.JSONDecodeError as e:
                log_with_context("ERROR", "Failed to parse request body", error=str(e))
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Invalid JSON in request body'})
                }
        
        # Get records or generate test data if none provided
        records = body.get('records', [])
        if not records:
            # Generate some test records if none provided
            records = [
                {'id': f'rec-{i}', 'value': i * 10, 'status': 'valid'} for i in range(30)
            ]
            # Add some invalid records
            records[5]['status'] = 'invalid'
            records[15]['status'] = 'invalid'
            
        log_with_context("INFO", "Processing input records", count=len(records))
        
        # Process the records
        result = process_records(records)
        
        # Log resource utilization
        if hasattr(context, 'get_remaining_time_in_millis'):
            remaining_time = context.get_remaining_time_in_millis()
            log_with_context("INFO", "Lambda resource utilization", 
                           remaining_time_ms=remaining_time,
                           used_time_ms=context.duration_ms - remaining_time if hasattr(context, 'duration_ms') else 'unknown')
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'message': 'Processing complete',
                'result': result,
                'request_id': request_id
            })
        }
        
    except Exception as e:
        log_with_context("ERROR", "Unhandled exception in Lambda handler", 
                        error=str(e),
                        traceback=traceback.format_exc(),
                        request_id=request_id)
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e),
                'request_id': request_id
            })
        }

# For local testing
if __name__ == "__main__":
    # Simulate the Lambda event and context
    test_event = {
        'body': json.dumps({
            'records': [
                {'id': f'rec-{i}', 'value': i * 10, 'status': 'valid'} for i in range(20)
            ]
        })
    }
    
    class MockContext:
        aws_request_id = str(uuid.uuid4())
        function_name = "test-function"
        memory_limit_in_mb = 128
        
        def get_remaining_time_in_millis(self):
            return 10000
    
    test_context = MockContext()
    
    # Execute the function
    print("Starting local test...")
    result = main(test_event, test_context)
    print(f"Result: {json.dumps(result, indent=2)}")
import json
import time
import uuid
import traceback
from datetime import datetime
from utils.logger import inject_logger_context, get_logger
from utils.observability import lambda_observability_decorator
from utils.audit_log_decorator import log_lambda_invocation

logger = get_logger(__name__)

def process_records(records):
    """Process some sample records and log the activity"""
    processed = 0
    errors = 0
    start_time = time.time()
    
    logger.info("Starting record processing", extra={
        "record_count": len(records),
        "process_id": str(uuid.uuid4())
    })
    
    for i, record in enumerate(records):
        try:
            # Simulate processing time
            time.sleep(0.1)
            
            # Log progress for long-running operations
            if (i + 1) % 10 == 0:
                logger.info("Processing progress", extra={
                    "processed": i+1,
                    "total": len(records),
                    "elapsed_sec": round(time.time() - start_time, 2)
                })
            
            # Simulate a business logic error for some records
            if record.get('status') == 'invalid':
                raise ValueError(f"Invalid record format: {record.get('id')}")
                
            # More business logic here...
            processed += 1
            
        except Exception as e:
            errors += 1
            logger.error("Failed to process record", extra={
                "record_id": record.get('id', 'unknown'),
                "error": str(e),
                "traceback": traceback.format_exc()
            })
    
    # Log completion with performance metrics
    duration = time.time() - start_time
    logger.info("Completed record processing", extra={
        "processed_count": processed,
        "error_count": errors,
        "duration_sec": round(duration, 2),
        "throughput": round(processed/duration, 2) if duration > 0 else 0
    })
    
    return {
        "processed": processed,
        "errors": errors,
        "duration_sec": round(duration, 2)
    }

# @lambda_observability_decorator('/test')
@inject_logger_context
@log_lambda_invocation()
def main(event, context, logger):
    """Main Lambda handler function"""
    request_id = context.aws_request_id if hasattr(context, 'aws_request_id') else str(uuid.uuid4())
    
    logger.info("Lambda function invoked", extra={
        "request_id": request_id,
        "function_name": context.function_name if hasattr(context, 'function_name') else 'unknown',
        "memory_limit_mb": context.memory_limit_in_mb if hasattr(context, 'memory_limit_in_mb') else 'unknown',
        "remaining_time_ms": context.get_remaining_time_in_millis() if hasattr(context, 'get_remaining_time_in_millis') else 'unknown'
    })
    
    try:
        # Parse the incoming event
        body = {}
        if event.get('body'):
            try:
                body = json.loads(event['body'])
                logger.info("Parsed request body", extra={"size_bytes": len(event['body'])})
            except json.JSONDecodeError as e:
                logger.error("Failed to parse request body", extra={"error": str(e)})
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
            
        logger.info("Processing input records", extra={"count": len(records)})
        
        # Process the records
        result = process_records(records)
        
        # Log resource utilization
        if hasattr(context, 'get_remaining_time_in_millis'):
            remaining_time = context.get_remaining_time_in_millis()
            logger.info("Lambda resource utilization", extra={
                "remaining_time_ms": remaining_time,
                "used_time_ms": context.duration_ms - remaining_time if hasattr(context, 'duration_ms') else 'unknown'
            })
        
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
        logger.error("Unhandled exception in Lambda handler", extra={
            "error": str(e),
            "traceback": traceback.format_exc(),
            "request_id": request_id
        })
        
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
    result = main(test_event, test_context, logger)
    print(f"Result: {json.dumps(result, indent=2)}")
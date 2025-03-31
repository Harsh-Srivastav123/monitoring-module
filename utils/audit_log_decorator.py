import json
import time
import os
import psutil
from datetime import datetime
from functools import wraps
from http import HTTPStatus
import pytz
from utils.logger import get_logger
from utils.db import insert_sql

# Use structured logger instead of basic logging
log = get_logger("audit_log_decorator")

# Global variable to detect cold start
is_cold_start = True

def log_lambda_invocation():
    """
    Decorator that logs Lambda invocation metrics and writes audit log entry
    to a PostgreSQL database table named "audit_logs".
    """
    def decorator(func):
        @wraps(func)
        def wrapper(event, context, *args, **kwargs):
            global is_cold_start
            # Record the start time and timestamp with timezone
            start_time = time.time()
            api_start_time = datetime.fromtimestamp(
                start_time, tz=pytz.timezone('Asia/Calcutta')
            )
            
            # Extract correlation_id from headers similar to logger.py
            correlation_id = None
            if isinstance(event, dict):
                headers = event.get("headers", {}) or {}
                if headers and "X-Correlation-ID" in headers:
                    correlation_id = headers["X-Correlation-ID"]
                elif "requestContext" in event and "requestId" in event.get("requestContext", {}):
                    correlation_id = event["requestContext"]["requestId"]
                elif headers and "X-Amzn-Trace-Id" in headers:
                    correlation_id = headers["X-Amzn-Trace-Id"]
            
            # Parse the request body
            try:
                body = json.loads(event.get("body", "{}"))
            except json.JSONDecodeError:
                body = {}

            # Extract some metrics from the event and context
            user_id = body.get("user_id")
            try:
                entity_name = context.function_name if hasattr(context, "function_name") else None
            except Exception:
                entity_name = None

            api_url = event.get('path')
            method_name = event.get('httpMethod')
            try:
                user_ip_address = event['requestContext']['identity'].get('sourceIp')
            except Exception:
                user_ip_address = None

            # Use psutil to get approximate memory usage (in MB) of the current process
            process = psutil.Process(os.getpid())
            memory_used_mb = process.memory_info().rss / (1024 * 1024)

            response = None
            error = None

            try:
                # Execute the main Lambda function
                response = func(event, context, *args, **kwargs)
                status_code = response.get('statusCode', HTTPStatus.INTERNAL_SERVER_ERROR)
            except Exception as e:
                error = e
                log.error(f"Error during Lambda invocation: {e}", exc_info=True)
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                response = {
                    'statusCode': status_code,
                    'body': json.dumps({'message': 'An error occurred during function execution.'}),
                    'error_message': str(e)
                }
            finally:
                # Record end time and compute duration
                end_time = time.time()
                duration_seconds = end_time - start_time
                api_end_time = datetime.fromtimestamp(
                    end_time, tz=pytz.timezone('Asia/Calcutta')
                )
                
                # Prepare the metric data with additional fields
                metric_data = {
                    "metric_type": "invocation",
                    "timestamp": api_start_time.isoformat(),
                    "endpoint": api_url,
                    "function_name": entity_name,
                    "status": status_code,
                    "duration_seconds": duration_seconds,
                    "memory_used_mb": memory_used_mb,
                    "cold_start": is_cold_start,
                    "http_method": method_name,
                    "user_id": user_id,
                    "user_ip_address": user_ip_address,
                    "correlation_id": correlation_id
                }
                log.info(f"Lambda Metrics: {json.dumps(metric_data)}")

                # Insert the audit log entry into Postgres
                try:
                    insert_query = """
                        INSERT INTO audit_logs (
                            method_name,
                            entity_name,
                            user_id,
                            api_end_point,
                            user_ip_address,
                            request_payload,
                            response,
                            response_status_code,
                            api_start_time,
                            api_end_time,
                            api_time_taken,
                            memory_used_mb,
                            cold_start,
                            correlation_id
                        ) VALUES (
                            %(method_name)s,
                            %(entity_name)s,
                            %(user_id)s,
                            %(api_end_point)s,
                            %(user_ip_address)s,
                            %(request_payload)s,
                            %(response)s,
                            %(response_status_code)s,
                            %(api_start_time)s,
                            %(api_end_time)s,
                            %(api_time_taken)s,
                            %(memory_used_mb)s,
                            %(cold_start)s,
                            %(correlation_id)s
                        )
                    """
                    log_entry = {
                        "method_name": method_name,
                        "entity_name": entity_name,
                        "user_id": user_id,
                        "api_end_point": api_url,
                        "user_ip_address": user_ip_address,
                        "request_payload": json.dumps(body) if not isinstance(body, str) else body,
                        "response": response.get("body") if response else None,
                        "response_status_code": status_code,
                        "api_start_time": api_start_time.isoformat(),
                        "api_end_time": api_end_time.isoformat(),
                        "api_time_taken": duration_seconds,
                        "memory_used_mb": memory_used_mb,
                        "cold_start": is_cold_start,
                        "correlation_id": correlation_id
                    }
                    # Use the insert_sql function from db.py instead of direct connection
                    insert_sql(insert_query, log_entry)
                except Exception as db_error:
                    log.error(f"Error while inserting audit log entry: {db_error}", exc_info=True)
                
                # Reset cold start flag after first invocation
                is_cold_start = False

            if error:
                raise error

            return response
        return wrapper
    return decorator
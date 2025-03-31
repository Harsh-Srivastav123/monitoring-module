import functools
import time
import json
import os
import uuid
import requests
from typing import Dict, Any, Callable

# Optional: You can install the AWS Lambda Powertools for more features
try:
    from aws_lambda_powertools import Logger
    logger = Logger()
except ImportError:
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

class LambdaMetricsCollector:
    def __init__(self):
        """Initialize the metrics collector"""
        self.metrics = []
        self.environment = os.environ.get('ENVIRONMENT', 'dev')
        self.pushgateway_url = os.environ.get('PUSHGATEWAY_URL')
        
    def record_invocation(self, endpoint: str, function_name: str, status: str, 
                         duration: float, memory_used: int = 0, 
                         is_cold_start: bool = False):
        """Record a Lambda invocation metric"""
        timestamp = time.time()
        
        metric_data = {
            "metric_type": "invocation",
            "timestamp": timestamp,
            "endpoint": endpoint,
            "function_name": function_name,
            "status": status,
            "duration_seconds": duration,
            "memory_used_mb": memory_used,
            "cold_start": is_cold_start,
            "environment": self.environment
        }
        
        # Log the metric for CloudWatch Logs
        logger.info(json.dumps({
            "message_type": "lambda_metrics",
            "metric_data": metric_data
        }))
        
        # Add to internal metrics store
        self.metrics.append(metric_data)
        
        # If configured, push directly to Pushgateway
        if self.pushgateway_url:
            self._push_to_gateway(metric_data)
    
    def record_error(self, endpoint: str, function_name: str, error_type: str):
        """Record an error metric"""
        timestamp = time.time()
        
        metric_data = {
            "metric_type": "error",
            "timestamp": timestamp,
            "endpoint": endpoint,
            "function_name": function_name,
            "error_type": error_type,
            "environment": self.environment
        }
        
        # Log the metric for CloudWatch Logs
        logger.info({
            "message_type": "lambda_metrics",
            "metric_data": metric_data
        })
        
        # Add to internal metrics store
        self.metrics.append(metric_data)
        
        # If configured, push directly to Pushgateway
        if self.pushgateway_url:
            self._push_to_gateway(metric_data)
    
    def _push_to_gateway(self, metric_data):
        """Push metrics to Prometheus Pushgateway"""
        try:
            prometheus_data = ""
            function_name = metric_data['function_name'].replace('-', '_')
            endpoint = metric_data['endpoint'].replace('/', '_')
            
            # Add timestamp to make each push unique
            timestamp = int(time.time())
            
            # Format metric data as Prometheus exposition format
            if metric_data['metric_type'] == 'invocation':
                # Use invocation event with timestamp in label
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

                # Add cold start with timestamp
                if metric_data.get('cold_start'):
                    prometheus_data += f"""
# TYPE lambda_cold_start_event gauge
lambda_cold_start_event{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",environment="{metric_data['environment']}",timestamp="{timestamp}"}} 1
"""
            
            elif metric_data['metric_type'] == 'error':
                prometheus_data += f"""
# TYPE lambda_error_event gauge
lambda_error_event{{endpoint="{metric_data['endpoint']}",function_name="{metric_data['function_name']}",error_type="{metric_data['error_type']}",environment="{metric_data['environment']}",timestamp="{timestamp}"}} 1
"""
            
            # Push to Pushgateway
            url = f"{self.pushgateway_url}/metrics/job/lambda_{function_name}/instance/{endpoint}"
            response = requests.post(url, data=prometheus_data)
            response.raise_for_status()
            logger.debug(f"Successfully pushed metrics to Pushgateway: {url}")
            
        except Exception as e:
            logger.error(f"Failed to push metrics to Pushgateway: {str(e)}")

# Create a global metrics collector instance
metrics_collector = LambdaMetricsCollector()

def lambda_observability_decorator(endpoint: str):
    """
    Decorator to add observability to Lambda functions
    
    :param endpoint: The API endpoint or identifier for this function
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(event: Dict[str, Any], context: Any):
            # Generate a unique trace ID for request correlation
            trace_id = str(uuid.uuid4())
            
            # Determine if this is a cold start
            is_cold_start = not hasattr(wrapper, '_warm')
            wrapper._warm = True
            
            # Get memory allocation
            memory_allocated = context.memory_limit_in_mb if hasattr(context, 'memory_limit_in_mb') else 0
            
            # Log the invocation
            logger.info({
                "message": "Lambda function invoked",
                "endpoint": endpoint,
                "function_name": context.function_name,
                "trace_id": trace_id,
                "cold_start": is_cold_start
            })
            
            # Start timing
            start_time = time.time()
            
            try:
                # Execute the function
                result = func(event, context)
                
                # Calculate duration
                duration = time.time() - start_time
                
                # Record successful invocation
                metrics_collector.record_invocation(
                    endpoint=endpoint,
                    function_name=context.function_name,
                    status="success",
                    duration=duration,
                    memory_used=memory_allocated,
                    is_cold_start=is_cold_start
                )
                
                # Log the success
                logger.info({
                    "message": "Lambda function completed",
                    "endpoint": endpoint,
                    "trace_id": trace_id,
                    "duration_seconds": duration
                })
                
                return result
                
            except Exception as e:
                # Calculate duration even on error
                duration = time.time() - start_time
                
                # Record error invocation
                metrics_collector.record_invocation(
                    endpoint=endpoint,
                    function_name=context.function_name,
                    status="error",
                    duration=duration,
                    memory_used=memory_allocated,
                    is_cold_start=is_cold_start
                )
                
                # Record specific error type
                metrics_collector.record_error(
                    endpoint=endpoint,
                    function_name=context.function_name,
                    error_type=type(e).__name__
                )
                
                # Log the error
                logger.error({
                    "message": "Lambda function failed",
                    "endpoint": endpoint,
                    "trace_id": trace_id,
                    "error_type": type(e).__name__,
                    "error": str(e),
                    "duration_seconds": duration
                })
                
                # Re-raise to maintain Lambda error handling
                raise
                
        return wrapper
    return decorator
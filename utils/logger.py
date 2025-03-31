# utils/logger.py

import json
import logging
import functools
import uuid
from enum import Enum
from aws_xray_sdk.core import xray_recorder  # Optional, if using AWS X-Ray

# Define an enum for log levels
class LogLevel(Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

# Custom JSON formatter that outputs structured log records
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "function": record.__dict__.get("function", "unknown"),
            "correlation_id": record.__dict__.get("correlation_id"),
            "xray_trace_id": (
                xray_recorder.current_segment().trace_id
                if xray_recorder.current_segment() else None
            )
        }
        return json.dumps(log_record)

# Get (or create) a logger with the given name
def get_logger(name="default"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return logger

# Decorator that injects logger context automatically into your Lambda handler.
# It extracts the correlation ID from event headers (using "X-Correlation-ID"),
# falls back to the API Gateway requestId, or generates a new UUID.
def inject_logger_context(func):
    @functools.wraps(func)
    def wrapper(event, context, *args, **kwargs):
        correlation_id = None
        if isinstance(event, dict):
            headers = event.get("headers", {})
            # Check for a custom correlation header
            if "X-Correlation-ID" in headers:
                correlation_id = headers["X-Correlation-ID"]
            # If not available, use the API Gateway request ID
            elif "requestContext" in event and "requestId" in event["requestContext"]:
                correlation_id = event["requestContext"]["requestId"]
            # Alternatively, check for the Amazon trace header
            elif "X-Amzn-Trace-Id" in headers:
                correlation_id = headers["X-Amzn-Trace-Id"]
        
        # Create a logger adapter that attaches the function name and correlation ID to every log
        logger = get_logger(func.__name__)
        adapter = logging.LoggerAdapter(logger, {
            "function": func.__name__,
            "correlation_id": correlation_id
        })
        
        adapter.info("Handler invoked")
        try:
            result = func(event, context, logger=adapter, *args, **kwargs)
            adapter.info("Handler completed successfully")
            return result
        except Exception as e:
            adapter.exception("Handler encountered an error")
            raise e
    return wrapper

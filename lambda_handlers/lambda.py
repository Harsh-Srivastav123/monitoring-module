# lambda_handlers/handler.py

import json
from utils.logger import inject_logger_context
from utils.audit_log_decorator import log_lambda_invocation

@inject_logger_context
@log_lambda_invocation()
def lambda_handler(event, context, logger):
    print(event)
    print(context)
    print(logger)
    # Log that the handler is invoked.
    logger.info("Lambda handler invoked")

    # Log some key attributes from the Lambda context.
    logger.debug(
        "Context details",
        extra={
            "function_name": context.function_name,
            "memory_limit_in_mb": context.memory_limit_in_mb,
            "aws_request_id": context.aws_request_id,
        }
    )

    # For a POST call, expect the payload in event["body"].
    try:
        body = json.loads(event.get("body", "{}"))
    except Exception as e:
        logger.error("Failed to parse request body", extra={"error": str(e)})
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON payload"})
        }
    
    # Log the received payload.
    logger.info("Received payload", extra={"payload": body})
    
    # Sample logical operation: Multiply two numbers if 'action' is 'multiply'.
    action = body.get("action", "").lower()
    if action == "multiply":
        try:
            a = float(body.get("a", 0))
            b = float(body.get("b", 0))
            logger.debug("Multiplying numbers", extra={"a": a, "b": b})
            result = a * b
            logger.info("Multiplication successful", extra={"result": result})
        except Exception as e:
            logger.error("Error during multiplication", extra={"error": str(e)})
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Calculation error"})
            }
    else:
        logger.warning("Unsupported action provided", extra={"action": action})
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Unsupported action"})
        }
    
    # Build and log the final response.
    response_body = {"message": "Operation completed", "result": result}
    logger.info("Returning response", extra={"response": response_body})
    return {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

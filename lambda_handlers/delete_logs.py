import json
from datetime import datetime, timedelta
import pytz
from utils.logger import get_logger
from utils.db import execute_sql
from utils.audit_log_decorator import log_lambda_invocation

# Initialize logger
log = get_logger("delete_logs")

@log_lambda_invocation()
@inject_logger_context
def handler(event, context):
    """
    Lambda handler to delete audit logs older than 48 hours with success status code (200).
    This function is scheduled to run daily at 11:30 PM.
    
    Args:
        event (dict): AWS Lambda event object
        context (object): AWS Lambda context object
        
    Returns:
        dict: Response with status code and deletion results
    """
    try:
        log.info("Starting deletion of old audit logs")
        
        # Calculate the cutoff time (48 hours ago)
        cutoff_time = datetime.now(pytz.timezone('Asia/Calcutta')) - timedelta(hours=48)
        cutoff_time_str = cutoff_time.isoformat()
        
        # SQL to delete old successful logs
        delete_query = """
            DELETE FROM audit_logs
            WHERE api_start_time < %(cutoff_time)s
            AND response_status_code = 200
            RETURNING id
        """
        
        # Parameters for the query
        params = {
            "cutoff_time": cutoff_time_str
        }
        
        # Execute the deletion
        result = execute_sql(delete_query, params, fetch_all=True)
        deleted_count = len(result) if result else 0
        
        log.info(f"Successfully deleted {deleted_count} audit logs older than {cutoff_time_str}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully deleted {deleted_count} audit logs',
                'deleted_logs': deleted_count,
                'cutoff_time': cutoff_time_str
            })
        }
        
    except Exception as e:
        log.error(f"Error deleting audit logs: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error occurred while deleting audit logs',
                'error': str(e)
            })
        }
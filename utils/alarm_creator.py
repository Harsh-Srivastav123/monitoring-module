import boto3
import json
from botocore.exceptions import ClientError

# Configuration parameters
REGION = 'ap-south-1'
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:235298594406:LambdaErrorAlert"

# Create clients for Lambda and CloudWatch
lambda_client = boto3.client('lambda', region_name=REGION)
cloudwatch_client = boto3.client('cloudwatch', region_name=REGION)

def alarm_exists(alarm_name):
    """
    Check if a CloudWatch alarm with the given name already exists.
    """
    try:
        response = cloudwatch_client.describe_alarms(AlarmNames=[alarm_name])
        return bool(response.get('MetricAlarms'))
    except ClientError as e:
        # In case of error, log it and treat as non-existent (or handle appropriately)
        print(f"Error checking alarm {alarm_name}: {e}")
        return False

def create_lambda_error_alarm(function_name):
    """
    Creates a CloudWatch alarm for the specified Lambda function.
    Returns a tuple (success_flag, message).
    """
    alarm_name = f"{function_name}-ErrorAlarm"
    try:
        cloudwatch_client.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"Alarm for errors in {function_name}",
            ActionsEnabled=True,
            AlarmActions=[SNS_TOPIC_ARN],
            MetricName='Errors',
            Namespace='AWS/Lambda',
            Statistic='Sum',
            Dimensions=[
                {
                    'Name': 'FunctionName',
                    'Value': function_name
                },
            ],
            Period=60,               # Check every 60 seconds
            EvaluationPeriods=1,     # Evaluate a single period
            Threshold=1,             # Alarm if one or more errors occur
            ComparisonOperator='GreaterThanOrEqualToThreshold'
        )
        return True, f"Alarm created for {function_name}"
    except ClientError as e:
        error_msg = f"Error creating alarm for {function_name}: {e}"
        print(error_msg)
        return False, error_msg

def list_lambda_functions():
    """
    Retrieves all Lambda function names in the account.
    """
    functions = []
    paginator = lambda_client.get_paginator('list_functions')
    for page in paginator.paginate():
        for function in page.get('Functions', []):
            functions.append(function['FunctionName'])
    return functions

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    Iterates through Lambda functions, checks for existing alarms,
    creates alarms as needed, and returns a JSON response with details.
    """
    results = []
    functions = list_lambda_functions()
    
    if not functions:
        results.append({"Message": "No Lambda functions found."})
    else:
        for function in functions:
            alarm_name = f"{function}-ErrorAlarm"
            if alarm_exists(alarm_name):
                results.append({
                    "FunctionName": function,
                    "Alarm": alarm_name,
                    "Status": "Already exists",
                    "Message": f"Alarm {alarm_name} already exists for {function}."
                })
            else:
                success, message = create_lambda_error_alarm(function)
                status = "Created" if success else "Error"
                results.append({
                    "FunctionName": function,
                    "Alarm": alarm_name,
                    "Status": status,
                    "Message": message
                })
    
    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }

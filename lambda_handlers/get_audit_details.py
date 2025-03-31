import json
import os
from datetime import datetime
from http import HTTPStatus
from typing import Dict, Any, Optional, List

import graphene
from graphene.types import datetime as graphene_datetime

from utils.logger import get_logger, inject_logger_context
from utils.db import fetch_all_sql, fetch_one_sql

# Initialize logger
log = get_logger("audit_details")

class AuditLogType(graphene.ObjectType):
    """Audit log entry type for GraphQL schema"""
    id = graphene.ID()
    method_name = graphene.String(description="HTTP method used in the request")
    entity_name = graphene.String(description="Lambda function name")
    user_id = graphene.String(description="User ID if available")
    api_end_point = graphene.String(description="API endpoint path")
    user_ip_address = graphene.String(description="User's IP address")
    request_payload = graphene.String(description="Request payload as JSON string")
    response = graphene.String(description="Response payload as JSON string")
    response_status_code = graphene.Int(description="HTTP response status code")
    api_start_time = graphene_datetime.DateTime(description="API call start time")
    api_end_time = graphene_datetime.DateTime(description="API call end time")
    api_time_taken = graphene.Float(description="API call duration in seconds")
    memory_used_mb = graphene.Float(description="Memory used in MB")
    cold_start = graphene.Boolean(description="Whether this was a cold start")
    correlation_id = graphene.String(description="Correlation ID for tracking requests")
    created_at = graphene_datetime.DateTime(description="Record creation timestamp")

class AuditLogFilterInput(graphene.InputObjectType):
    """Input type for filtering audit logs"""
    user_id = graphene.String()
    method_name = graphene.String()
    entity_name = graphene.String()
    api_end_point = graphene.String()
    correlation_id = graphene.String()
    start_date = graphene_datetime.DateTime()
    end_date = graphene_datetime.DateTime()
    min_duration = graphene.Float()
    status_code = graphene.Int()
    cold_start = graphene.Boolean()

class Query(graphene.ObjectType):
    """Root query for audit logs"""
    
    # Get a single audit log by ID
    audit_log = graphene.Field(
        AuditLogType, 
        id=graphene.ID(required=True, description="ID of the audit log entry")
    )
    
    # Get multiple audit logs with filtering options
    audit_logs = graphene.List(
        AuditLogType,
        filter=graphene.Argument(AuditLogFilterInput, description="Filter criteria"),
        limit=graphene.Int(description="Maximum number of results to return", default_value=100),
        offset=graphene.Int(description="Number of results to skip", default_value=0)
    )

    def resolve_audit_log(self, info, id):
        """Resolver for fetching a single audit log by ID"""
        try:
            query = """
                SELECT * FROM audit_logs 
                WHERE id = %s
            """
            log.info(f"Fetching audit log with ID: {id}")
            result = fetch_one_sql(query, (id,))
            
            if not result:
                return None
                
            # Convert datetime objects to make them serializable
            for key, value in result.items():
                if isinstance(value, datetime):
                    result[key] = value
                    
            return AuditLogType(**result)
        except Exception as e:
            log.error(f"Error fetching audit log: {str(e)}")
            return None

    def resolve_audit_logs(self, info, filter=None, limit=100, offset=0):
        """Resolver for fetching multiple audit logs with filtering"""
        try:
            # Start building the query
            query = "SELECT * FROM audit_logs WHERE 1=1"
            params = []
            
            # Apply filters if provided
            if filter:
                if filter.user_id:
                    query += " AND user_id = %s"
                    params.append(filter.user_id)
                    
                if filter.method_name:
                    query += " AND method_name = %s"
                    params.append(filter.method_name)
                    
                if filter.entity_name:
                    query += " AND entity_name = %s"
                    params.append(filter.entity_name)
                    
                if filter.api_end_point:
                    query += " AND api_end_point LIKE %s"
                    params.append(f"%{filter.api_end_point}%")
                    
                if filter.correlation_id:
                    query += " AND correlation_id = %s"
                    params.append(filter.correlation_id)
                    
                if filter.start_date:
                    query += " AND api_start_time >= %s"
                    params.append(filter.start_date)
                    
                if filter.end_date:
                    query += " AND api_end_time <= %s"
                    params.append(filter.end_date)
                    
                if filter.min_duration is not None:
                    query += " AND api_time_taken >= %s"
                    params.append(filter.min_duration)
                    
                if filter.status_code:
                    query += " AND response_status_code = %s"
                    params.append(filter.status_code)
                    
                if filter.cold_start is not None:
                    query += " AND cold_start = %s"
                    params.append(filter.cold_start)
            
            # Add sorting, limit, and offset
            query += " ORDER BY api_start_time DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            log.info(f"Executing query: {query} with params: {params}")
            results = fetch_all_sql(query, params)
            
            if not results:
                return []
                
            audit_logs = []
            for result in results:
                # Convert datetime objects for serialization
                for key, value in result.items():
                    if isinstance(value, datetime):
                        result[key] = value
                audit_logs.append(AuditLogType(**result))
                
            return audit_logs
        except Exception as e:
            log.error(f"Error fetching audit logs: {str(e)}")
            return []

# Create schema
schema = graphene.Schema(query=Query)

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler for the GraphQL API"""
    try:
        # Handle CORS preflight requests
        if event.get('httpMethod') == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS'
                },
                'body': ''
            }

        # Parse request body
        body = {}
        if event.get('body'):
            if isinstance(event['body'], str):
                body = json.loads(event['body'])
            else:
                body = event['body']
                
        query = body.get('query')
        variables = body.get('variables', {})

        if not query:
            log.error("No GraphQL query provided")
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'No GraphQL query provided'})
            }

        # Execute the GraphQL query
        result = schema.execute(query, variable_values=variables)

        if result.errors:
            log.error(f"GraphQL errors: {result.errors}")
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'errors': [str(error) for error in result.errors]})
            }

        log.info("GraphQL query executed successfully")
        return {
            'statusCode': HTTPStatus.OK,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'data': result.data})
        }

    except Exception as e:
        log.error(f"Handler error: {str(e)}", exc_info=True)
        return {
            'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': str(e)})
        }
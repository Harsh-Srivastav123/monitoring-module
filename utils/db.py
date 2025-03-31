import json
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from utils.logger import get_logger
from utils.secrets_manager import get_credentials

# Initialize logger
log = get_logger("database")

@contextmanager
def get_db_connection():
    """
    Context manager to handle database connections.
    Automatically closes the connection when operation is complete.
    """
    conn = None
    try:
        # Get database credentials from secrets manager
        db_credentials = get_credentials("database_credentials")
        
        # Connection parameters from secrets manager
        conn = psycopg2.connect(
            host=db_credentials.get("host"),
            database=db_credentials.get("dbname"), 
            user=db_credentials.get("username"),
            password=db_credentials.get("password")
        )
        yield conn
    except Exception as e:
        log.error(f"Database connection error: {e}", exc_info=True)
        raise
    finally:
        if conn is not None:
            conn.close()

def execute_sql(sql, params=None, fetch_one=False, fetch_all=False):
    """
    Execute SQL query and optionally return results.
    
    Args:
        sql (str): SQL query to execute
        params (dict, optional): Parameters for the SQL query
        fetch_one (bool): Whether to fetch a single row
        fetch_all (bool): Whether to fetch all rows
        
    Returns:
        The query result based on fetch parameters, or None for non-select operations
    """
    try:
        log.info(f"Executing SQL query: {sql}")
        log.info(f"Query parameters: {json.dumps(params) if params else None}")
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params or {})
                
                if fetch_one:
                    result = cursor.fetchone()
                    log.info(f"Fetched single row result: {result}")
                    return dict(result) if result else None
                elif fetch_all:
                    results = cursor.fetchall()
                    log.info(f"Fetched {len(results) if results else 0} rows")
                    return [dict(row) for row in results] if results else []
                else:
                    conn.commit()
                    log.info("Query executed successfully with commit")
                    return None
    except Exception as e:
        log.error(f"SQL execution error: {e}", exc_info=True)
        log.error(f"Query: {sql}")
        log.error(f"Params: {json.dumps(params) if params else None}")
        raise

def fetch_one_sql(sql, params=None):
    """
    Fetch a single row from the database.
    
    Args:
        sql (str): SQL query to execute
        params (dict, optional): Parameters for the SQL query
        
    Returns:
        dict: The fetched row as a dictionary, or None if no row is found
    """
    log.info(f"Fetching single row with query: {sql}")
    log.info(f"Parameters: {json.dumps(params) if params else None}")
    return execute_sql(sql, params, fetch_one=True)

def fetch_all_sql(sql, params=None):
    """
    Fetch all rows from the database.
    
    Args:
        sql (str): SQL query to execute
        params (dict, optional): Parameters for the SQL query
        
    Returns:
        list: List of dictionaries representing the fetched rows
    """
    log.info(f"Fetching all rows with query: {sql}")
    log.info(f"Parameters: {json.dumps(params) if params else None}")
    return execute_sql(sql, params, fetch_all=True)

def insert_sql(sql, params=None):
    """
    Insert data into the database.
    
    Args:
        sql (str): SQL insert query to execute
        params (dict, optional): Parameters for the SQL query
        
    Returns:
        None
    """
    log.info(f"Executing insert query: {sql}")
    log.info(f"Parameters: {json.dumps(params) if params else None}")
    return execute_sql(sql, params)
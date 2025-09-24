import psycopg2
import logging
import time
from typing import List, Dict, Any
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

class SynchronousExecutor:
    """Execute queries using traditional synchronous I/O (baseline)"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.method_name = "synchronous"
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a single query using synchronous I/O"""
        conn = None
        cursor = None
        try:
            conn = self.db_connection.get_sync_connection()
            cursor = conn.cursor()
            
            # Configure for synchronous execution
            cursor.execute("SET synchronous_commit = on")
            
            start_time = time.time()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result_dicts = [dict(zip(columns, row)) for row in result]
            
            execution_time = time.time() - start_time
            
            logger.debug(f"Synchronous query executed in {execution_time:.4f}s")
            
            return result_dicts, execution_time
            
        except Exception as e:
            logger.error(f"Synchronous query execution failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_connection.return_sync_connection(conn)
    
    def execute_queries_sequential(self, queries: List[str]) -> List[float]:
        """Execute queries sequentially (synchronous baseline)"""
        execution_times = []
        
        for query in queries:
            try:
                result, exec_time = self.execute_query(query)
                execution_times.append(exec_time)
            except Exception as e:
                logger.error(f"Query failed: {e}")
                execution_times.append(float('inf'))
        
        return execution_times
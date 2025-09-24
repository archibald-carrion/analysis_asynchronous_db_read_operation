import asyncpg
import logging
import time
from typing import List, Dict, Any
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

class BackgroundWorkersExecutor:
    """Execute queries using PostgreSQL background workers"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.method_name = "background_workers"
    
    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a single query using background workers"""
        conn = None
        try:
            conn = await self.db_connection.get_async_connection()
            
            # Configure for parallel execution
            await conn.execute("SET max_parallel_workers_per_gather = 4")
            await conn.execute("SET parallel_setup_cost = 0")
            await conn.execute("SET parallel_tuple_cost = 0")
            
            start_time = time.time()
            
            if params:
                result = await conn.fetch(query, *params.values())
            else:
                result = await conn.fetch(query)
            
            execution_time = time.time() - start_time
            
            logger.debug(f"Background workers query executed in {execution_time:.4f}s")
            
            return [dict(row) for row in result], execution_time
            
        except Exception as e:
            logger.error(f"Background workers query execution failed: {e}")
            raise
        finally:
            if conn:
                await self.db_connection.return_async_connection(conn)
    
    async def execute_queries_parallel(self, queries: List[str]) -> List[float]:
        """Execute queries in parallel using background workers"""
        tasks = []
        
        for query in queries:
            task = self.execute_query(query)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        execution_times = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Query failed: {result}")
                execution_times.append(float('inf'))
            else:
                execution_times.append(result[1])
        
        return execution_times
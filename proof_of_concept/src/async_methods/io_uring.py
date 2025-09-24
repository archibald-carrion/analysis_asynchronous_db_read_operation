import asyncpg
import logging
import time
from typing import List, Dict, Any
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

class IOuringAsyncExecutor:
    """Execute queries using PostgreSQL 18's io_uring async I/O"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.method_name = "io_uring"
    
    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a single query using io_uring async I/O"""
        conn = None
        try:
            conn = await self.db_connection.get_async_connection()
            
            # Configure connection for io_uring
            await conn.execute("SET io_uring.async_read = on")
            await conn.execute("SET io_uring.async_write = on")
            
            start_time = time.time()
            
            if params:
                result = await conn.fetch(query, *params.values())
            else:
                result = await conn.fetch(query)
            
            execution_time = time.time() - start_time
            
            logger.debug(f"IOuring query executed in {execution_time:.4f}s")
            
            return [dict(row) for row in result], execution_time
            
        except Exception as e:
            logger.error(f"IOuring query execution failed: {e}")
            raise
        finally:
            if conn:
                await self.db_connection.return_async_connection(conn)
    
    async def execute_queries_concurrent(self, queries: List[str], max_concurrent: int = 5) -> List[float]:
        """Execute multiple queries concurrently"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def execute_with_semaphore(query):
            async with semaphore:
                return await self.execute_query(query)
        
        tasks = [execute_with_semaphore(query) for query in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        execution_times = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Query failed: {result}")
                execution_times.append(float('inf'))
            else:
                execution_times.append(result[1])
        
        return execution_times
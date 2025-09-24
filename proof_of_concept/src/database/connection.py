import asyncpg
import psycopg2
from psycopg2 import pool
import yaml
import asyncio
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manage database connections for different async methods"""
    
    def __init__(self, config_path: str = "config/database.yaml"):
        self.config = self._load_config(config_path)
        self.sync_pool = None
        self.async_pool = None
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load database configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            raise
    
    def get_sync_connection(self):
        """Get synchronous database connection"""
        if self.sync_pool is None:
            self._create_sync_pool()
        
        return self.sync_pool.getconn()
    
    def return_sync_connection(self, conn):
        """Return synchronous connection to pool"""
        self.sync_pool.putconn(conn)
    
    def _create_sync_pool(self):
        """Create synchronous connection pool"""
        db_config = self.config['postgresql']
        self.sync_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=db_config['pool_size'],
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
    
    async def get_async_connection(self):
        """Get asynchronous database connection"""
        if self.async_pool is None:
            await self._create_async_pool()
        
        return await self.async_pool.acquire()
    
    async def return_async_connection(self, conn):
        """Return asynchronous connection to pool"""
        await self.async_pool.release(conn)
    
    async def _create_async_pool(self):
        """Create asynchronous connection pool"""
        db_config = self.config['postgresql']
        self.async_pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password'],
            min_size=1,
            max_size=db_config['pool_size']
        )
    
    def configure_async_io(self, connection, method: str):
        """Configure PostgreSQL for specific async I/O method"""
        config_queries = {
            'io_uring': [
                "SET io_uring.async_read = on",
                "SET io_uring.async_write = on",
                "SET io_uring.entries = %s" % self.config['postgresql']['async_settings']['io_uring_entries']
            ],
            'background_workers': [
                "SET max_parallel_workers = %s" % self.config['postgresql']['async_settings']['background_workers'],
                "SET max_worker_processes = %s" % (self.config['postgresql']['async_settings']['background_workers'] + 10)
            ],
            'synchronous': [
                "SET synchronous_commit = on"
            ]
        }
        
        for query in config_queries.get(method, []):
            connection.execute(query)
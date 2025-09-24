import logging
import os
from typing import Optional
from .connection import DatabaseConnection

logger = logging.getLogger(__name__)

class TPCHSchemaCreator:
    """Create and manage TPC-H database schema"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.schema_file = "config/tpch_schema.sql"
    
    def create_schema(self) -> bool:
        """Create complete TPC-H schema"""
        try:
            conn = self.db_connection.get_sync_connection()
            cursor = conn.cursor()
            
            # Read and execute schema SQL
            with open(self.schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:  # Skip empty statements
                    try:
                        cursor.execute(statement)
                        logger.info(f"Executed: {statement[:50]}...")
                    except Exception as e:
                        logger.warning(f"Statement failed: {e}")
                        continue
            
            conn.commit()
            cursor.close()
            self.db_connection.return_sync_connection(conn)
            
            logger.info("TPC-H schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            return False
    
    def drop_schema(self) -> bool:
        """Drop all TPC-H tables"""
        try:
            conn = self.db_connection.get_sync_connection()
            cursor = conn.cursor()
            
            tables = [
                'lineitem', 'orders', 'customer', 
                'partsupp', 'part', 'supplier', 
                'nation', 'region'
            ]
            
            for table in tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.info(f"Dropped table: {table}")
                except Exception as e:
                    logger.warning(f"Failed to drop {table}: {e}")
            
            conn.commit()
            cursor.close()
            self.db_connection.return_sync_connection(conn)
            
            logger.info("TPC-H schema dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop schema: {e}")
            return False
    
    def schema_exists(self) -> bool:
        """Check if TPC-H schema exists"""
        try:
            conn = self.db_connection.get_sync_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'region'
                );
            """)
            
            exists = cursor.fetchone()[0]
            cursor.close()
            self.db_connection.return_sync_connection(conn)
            
            return exists
            
        except Exception as e:
            logger.error(f"Failed to check schema existence: {e}")
            return False
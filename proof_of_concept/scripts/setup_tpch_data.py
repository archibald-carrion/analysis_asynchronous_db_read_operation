#!/usr/bin/env python3
"""Setup TPC-H database with generated data"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / 'src'))

from src.database.connection import DatabaseConnection
from src.database.schema_creator import TPCHSchemaCreator
from src.database.data_generator import TPCHDataGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Setup TPC-H database')
    parser.add_argument('--scale-factor', type=float, default=1.0, help='TPC-H scale factor')
    parser.add_argument('--create-indexes', action='store_true', help='Create indexes after data load')
    parser.add_argument('--drop-existing', action='store_true', help='Drop existing schema')
    
    args = parser.parse_args()
    
    try:
        # Initialize database connection
        db_connection = DatabaseConnection()
        schema_creator = TPCHSchemaCreator(db_connection)
        data_generator = TPCHDataGenerator(db_connection)
        
        # Drop existing schema if requested
        if args.drop_existing:
            logger.info("Dropping existing schema...")
            schema_creator.drop_schema()
        
        # Create schema
        logger.info("Creating TPC-H schema...")
        if not schema_creator.create_schema():
            logger.error("Failed to create schema")
            return 1
        
        # Generate data
        logger.info(f"Generating TPC-H data (scale factor: {args.scale_factor})...")
        if not data_generator.generate_data(args.scale_factor):
            logger.error("Failed to generate data")
            return 1
        
        # Create indexes if requested
        if args.create_indexes:
            logger.info("Creating indexes...")
            if not data_generator.create_indexes():
                logger.warning("Failed to create some indexes")
        
        logger.info("TPC-H database setup completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
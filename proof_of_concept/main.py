#!/usr/bin/env python3
"""
TPC-H PostgreSQL 18 Async Benchmark
Main application entry point
"""

import logging
import asyncio
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from src.database.connection import DatabaseConnection
from src.database.schema_creator import TPCHSchemaCreator
from src.benchmark.runner import BenchmarkRunner
from src.gui.main_window import BenchmarkGUI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('benchmark.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class TPCHBenchmarkApp:
    """Main application class for TPC-H benchmark"""
    
    def __init__(self):
        self.db_connection = None
        self.schema_creator = None
        self.benchmark_runner = None
    
    async def initialize(self):
        """Initialize the application"""
        try:
            self.db_connection = DatabaseConnection()
            self.schema_creator = TPCHSchemaCreator(self.db_connection)
            self.benchmark_runner = BenchmarkRunner()
            
            logger.info("TPC-H Benchmark application initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            return False
    
    async def setup_database(self, scale_factor: int = 1) -> bool:
        """Set up TPC-H database schema and data"""
        try:
            logger.info("Setting up TPC-H database...")
            
            # Create schema
            if not self.schema_creator.create_schema():
                logger.error("Failed to create schema")
                return False
            
            # Generate data (simplified - in real implementation, use TPC-H dbgen)
            logger.info(f"Database setup complete for scale factor {scale_factor}")
            return True
            
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            return False
    
    async def run_benchmark_cli(self, args):
        """Run benchmark from command line"""
        if not await self.initialize():
            return False
        
        if args.setup_database:
            if not await self.setup_database(args.scale_factor):
                return False
        
        logger.info("Starting benchmark...")
        results = await self.benchmark_runner.run_benchmark()
        
        logger.info("Benchmark completed successfully")
        return True
    
    def run_gui(self):
        """Run the graphical user interface"""
        try:
            gui = BenchmarkGUI(self)
            gui.run()
        except Exception as e:
            logger.error(f"GUI failed to start: {e}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='TPC-H PostgreSQL 18 Async Benchmark')
    parser.add_argument('--gui', action='store_true', help='Run with GUI interface')
    parser.add_argument('--setup-database', action='store_true', help='Set up database before benchmark')
    parser.add_argument('--scale-factor', type=int, default=1, help='TPC-H scale factor')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    
    args = parser.parse_args()
    
    app = TPCHBenchmarkApp()
    
    if args.gui and not args.headless:
        # Run GUI
        app.run_gui()
    else:
        # Run CLI
        asyncio.run(app.run_benchmark_cli(args))

if __name__ == "__main__":
    main()
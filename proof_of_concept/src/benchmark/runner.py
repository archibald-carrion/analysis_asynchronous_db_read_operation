import asyncio
import logging
import time
from typing import Dict, List, Any
import yaml
import pandas as pd
from pathlib import Path

from ..async_methods.io_uring import IOuringAsyncExecutor
from ..async_methods.background_workers import BackgroundWorkersExecutor
from ..async_methods.synchronous import SynchronousExecutor
from ..database.connection import DatabaseConnection
from .queries import TPC_H_QUERIES

logger = logging.getLogger(__name__)

class BenchmarkRunner:
    """Main benchmark runner for TPC-H async methods"""
    
    def __init__(self, config_path: str = "config/benchmark.yaml"):
        self.config = self._load_config(config_path)
        self.db_connection = DatabaseConnection()
        self.results = {}
        
        # Initialize executors
        self.executors = {
            'io_uring': IOuringAsyncExecutor(self.db_connection),
            'background_workers': BackgroundWorkersExecutor(self.db_connection),
            'synchronous': SynchronousExecutor(self.db_connection)
        }
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load benchmark configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Benchmark config not found: {config_path}")
            raise
    
    async def run_benchmark(self) -> Dict[str, Any]:
        """Run complete benchmark suite"""
        benchmark_config = self.config['benchmark']
        methods = benchmark_config['methods']
        concurrent_levels = benchmark_config['concurrent_queries']
        
        overall_results = {}
        
        for method in methods:
            if method not in self.executors:
                logger.warning(f"Unknown method: {method}")
                continue
                
            logger.info(f"Running benchmark for method: {method}")
            method_results = await self._benchmark_method(method, concurrent_levels)
            overall_results[method] = method_results
        
        self.results = overall_results
        await self._generate_reports()
        
        return overall_results
    
    async def _benchmark_method(self, method: str, concurrent_levels: List[int]) -> Dict[str, Any]:
        """Benchmark a specific method at different concurrency levels"""
        executor = self.executors[method]
        results = {}
        
        # Test individual queries
        query_results = await self._test_individual_queries(executor)
        results['individual_queries'] = query_results
        
        # Test concurrent execution
        for concurrency in concurrent_levels:
            logger.info(f"Testing {method} with concurrency {concurrency}")
            concurrent_results = await self._test_concurrent_queries(executor, concurrency)
            results[f'concurrent_{concurrency}'] = concurrent_results
        
        return results
    
    async def _test_individual_queries(self, executor) -> Dict[str, float]:
        """Test each TPC-H query individually"""
        results = {}
        
        for query_name, query in TPC_H_QUERIES.items():
            logger.info(f"Testing query: {query_name}")
            
            try:
                if isinstance(executor, SynchronousExecutor):
                    # Synchronous execution
                    result, exec_time = executor.execute_query(query)
                else:
                    # Asynchronous execution
                    result, exec_time = await executor.execute_query(query)
                
                results[query_name] = exec_time
                logger.info(f"{query_name} completed in {exec_time:.4f}s")
                
            except Exception as e:
                logger.error(f"Query {query_name} failed: {e}")
                results[query_name] = float('inf')
        
        return results
    
    async def _test_concurrent_queries(self, executor, concurrency: int) -> Dict[str, Any]:
        """Test concurrent query execution"""
        # Use a mix of simple and complex queries
        test_queries = [
            TPC_H_QUERIES['Q1'],  # Simple
            TPC_H_QUERIES['Q6'],  # Medium
            TPC_H_QUERIES['Q9'],  # Complex
            TPC_H_QUERIES['Q13'], # Complex
        ] * (concurrency // 4 + 1)  # Repeat to reach desired concurrency
        test_queries = test_queries[:concurrency]
        
        start_time = time.time()
        
        try:
            if isinstance(executor, SynchronousExecutor):
                execution_times = executor.execute_queries_sequential(test_queries)
            else:
                execution_times = await executor.execute_queries_concurrent(test_queries, concurrency)
            
            total_time = time.time() - start_time
            avg_time = sum(execution_times) / len(execution_times)
            
            return {
                'total_time': total_time,
                'average_time': avg_time,
                'query_times': execution_times,
                'queries_completed': len([t for t in execution_times if t < float('inf')])
            }
            
        except Exception as e:
            logger.error(f"Concurrent test failed: {e}")
            return {
                'total_time': float('inf'),
                'average_time': float('inf'),
                'query_times': [],
                'queries_completed': 0
            }
    
    async def _generate_reports(self):
        """Generate benchmark reports"""
        output_dir = Path(self.config['benchmark']['output']['directory'])
        output_dir.mkdir(exist_ok=True)
        
        timestamp = time.strftime(self.config['benchmark']['output']['timestamp_format'])
        
        # Generate CSV report
        if 'csv' in self.config['benchmark']['output']['format']:
            self._generate_csv_report(output_dir / f"benchmark_results_{timestamp}.csv")
        
        # Generate JSON report
        if 'json' in self.config['benchmark']['output']['format']:
            self._generate_json_report(output_dir / f"benchmark_results_{timestamp}.json")
    
    def _generate_csv_report(self, filepath: Path):
        """Generate CSV report"""
        rows = []
        
        for method, results in self.results.items():
            # Individual query results
            if 'individual_queries' in results:
                for query_name, exec_time in results['individual_queries'].items():
                    rows.append({
                        'method': method,
                        'query': query_name,
                        'execution_time': exec_time,
                        'test_type': 'individual'
                    })
            
            # Concurrent results
            for key, result in results.items():
                if key.startswith('concurrent_'):
                    concurrency = int(key.split('_')[1])
                    rows.append({
                        'method': method,
                        'query': f'concurrent_{concurrency}',
                        'execution_time': result['average_time'],
                        'total_time': result['total_time'],
                        'queries_completed': result['queries_completed'],
                        'test_type': 'concurrent'
                    })
        
        df = pd.DataFrame(rows)
        df.to_csv(filepath, index=False)
        logger.info(f"CSV report generated: {filepath}")
    
    def _generate_json_report(self, filepath: Path):
        """Generate JSON report"""
        import json
        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"JSON report generated: {filepath}")
"""
Benchmark Suite

This script benchmarks query performance between InnoDB and ColumnStore engines.
It executes all analytical queries on both engines, measures execution times,
compares results, and generates a detailed performance report.
"""

import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from tabulate import tabulate
import sys

from config import (
    INNODB_TABLE, COLUMNSTORE_TABLE, BENCHMARK_CONFIG,
    RESULTS_DIR, TIMESTAMP_FORMAT
)
from db_connector import DatabaseConnection, compare_storage
from queries import QUERIES
from utils import format_time, calculate_speedup, compare_results


class BenchmarkRunner:
    """
    Runs performance benchmarks comparing InnoDB and ColumnStore.

    Executes all queries on both engines, measures performance,
    validates results, and generates reports.
    """

    def __init__(self):
        """Initialize the benchmark runner."""
        self.innodb_conn = None
        self.columnstore_conn = None
        self.results = []
        self.storage_metrics = None

    def setup(self) -> None:
        """
        Set up database connections and retrieve storage metrics.
        """
        print("=" * 70)
        print("FlightLake Benchmark Suite")
        print("=" * 70)
        print()

        # Initialize connections
        self.innodb_conn = DatabaseConnection(INNODB_TABLE)
        self.columnstore_conn = DatabaseConnection(COLUMNSTORE_TABLE)

        self.innodb_conn.connect()
        self.columnstore_conn.connect()

        # Get storage metrics
        print("\nRetrieving storage metrics...")
        self.storage_metrics = compare_storage(INNODB_TABLE, COLUMNSTORE_TABLE)
        self.print_storage_comparison()
        print()

    def print_storage_comparison(self) -> None:
        """Print storage size comparison between engines."""
        print("Storage Comparison:")
        print("-" * 70)

        innodb_info = self.storage_metrics['innodb']
        cs_info = self.storage_metrics['columnstore']
        ratio = self.storage_metrics['compression_ratio']

        print(f"  InnoDB Table:      {innodb_info.get('total_mb', 0):,.0f} MB")
        print(f"  ColumnStore Table: {cs_info.get('total_mb', 0):,.0f} MB")
        print(f"  Compression Ratio: {ratio:.1f}x")
        print()

    def run_query_benchmark(self, query_key: str, query_info: dict) -> dict:
        """
        Benchmark a single query on both engines.

        Args:
            query_key: Query identifier
            query_info: Query metadata and SQL

        Returns:
            Dictionary with benchmark results
        """
        print(f"Query: {query_info['name']}")
        print(f"  Description: {query_info['description']}")
        print()

        # Format SQL for each table
        innodb_sql = query_info['sql'].format(table_name=INNODB_TABLE)
        columnstore_sql = query_info['sql'].format(table_name=COLUMNSTORE_TABLE)

        # Warmup runs
        if BENCHMARK_CONFIG['warmup_runs'] > 0:
            print("  Running warmup...")
            for _ in range(BENCHMARK_CONFIG['warmup_runs']):
                self.innodb_conn.execute_query(innodb_sql)
                self.columnstore_conn.execute_query(columnstore_sql)

        # Benchmark InnoDB
        if BENCHMARK_CONFIG['cache_clear']:
            self.innodb_conn.clear_cache()

        print("  [InnoDB] Running...", end=" ", flush=True)
        innodb_times = []
        innodb_results = None

        for _ in range(BENCHMARK_CONFIG['test_runs']):
            start = time.time()
            innodb_results = self.innodb_conn.execute_query(innodb_sql)
            end = time.time()
            innodb_times.append(end - start)

        innodb_avg_time = sum(innodb_times) / len(innodb_times)
        print(f"Done in {format_time(innodb_avg_time)}")

        # Benchmark ColumnStore
        if BENCHMARK_CONFIG['cache_clear']:
            self.columnstore_conn.clear_cache()

        print("  [ColumnStore] Running...", end=" ", flush=True)
        columnstore_times = []
        columnstore_results = None

        for _ in range(BENCHMARK_CONFIG['test_runs']):
            start = time.time()
            columnstore_results = self.columnstore_conn.execute_query(columnstore_sql)
            end = time.time()
            columnstore_times.append(end - start)

        columnstore_avg_time = sum(columnstore_times) / len(columnstore_times)
        print(f"Done in {format_time(columnstore_avg_time)}")

        # Compare results
        results_match = compare_results(innodb_results, columnstore_results)
        speedup = calculate_speedup(innodb_avg_time, columnstore_avg_time)

        winner = "ColumnStore" if columnstore_avg_time < innodb_avg_time else "InnoDB"
        print(f"  Winner: {winner} ({speedup:.1f}x faster)")
        print(f"  Results match: {'Yes' if results_match else 'No'}")

        if not results_match:
            print(f"  WARNING: Results differ between engines!")
            print(f"    InnoDB rows: {len(innodb_results) if innodb_results else 0}")
            print(f"    ColumnStore rows: {len(columnstore_results) if columnstore_results else 0}")

        print()
        print("-" * 70)
        print()

        # Return benchmark results
        return {
            'query_name': query_info['name'],
            'query_key': query_key,
            'description': query_info['description'],
            'category': query_info.get('category', 'Unknown'),
            'innodb_time_sec': innodb_avg_time,
            'columnstore_time_sec': columnstore_avg_time,
            'speedup': speedup,
            'rows_returned': len(innodb_results) if innodb_results else 0,
            'results_match': results_match,
            'winner': winner
        }

    def run_all_benchmarks(self) -> None:
        """
        Execute benchmarks for all queries.
        """
        print("Running benchmarks...")
        print("=" * 70)
        print()

        for idx, (query_key, query_info) in enumerate(QUERIES.items(), 1):
            print(f"[{idx}/{len(QUERIES)}] ", end="")

            result = self.run_query_benchmark(query_key, query_info)
            self.results.append(result)

    def print_summary(self) -> None:
        """
        Print benchmark summary table.
        """
        print()
        print("=" * 70)
        print("Benchmark Summary")
        print("=" * 70)
        print()

        # Prepare table data
        table_data = []
        for r in self.results:
            table_data.append([
                r['query_name'][:30],  # Truncate long names
                format_time(r['innodb_time_sec']),
                format_time(r['columnstore_time_sec']),
                f"{r['speedup']:.1f}x",
                r['winner'],
                'Yes' if r['results_match'] else 'No'
            ])

        headers = ['Query', 'InnoDB', 'ColumnStore', 'Speedup', 'Winner', 'Match']
        print(tabulate(table_data, headers=headers, tablefmt='grid'))
        print()

        # Calculate statistics
        speedups = [r['speedup'] for r in self.results]
        avg_speedup = sum(speedups) / len(speedups)
        max_speedup = max(speedups)
        min_speedup = min(speedups)

        columnstore_wins = sum(1 for r in self.results if r['winner'] == 'ColumnStore')
        innodb_wins = sum(1 for r in self.results if r['winner'] == 'InnoDB')

        print("Statistics:")
        print(f"  Average Speedup: {avg_speedup:.1f}x")
        print(f"  Max Speedup: {max_speedup:.1f}x")
        print(f"  Min Speedup: {min_speedup:.1f}x")
        print(f"  ColumnStore Wins: {columnstore_wins}/{len(self.results)}")
        print(f"  InnoDB Wins: {innodb_wins}/{len(self.results)}")
        print()

        # Storage summary
        innodb_size = self.storage_metrics['innodb'].get('total_mb', 0)
        cs_size = self.storage_metrics['columnstore'].get('total_mb', 0)
        compression = self.storage_metrics['compression_ratio']

        print(f"Storage: InnoDB {innodb_size:,.0f} MB → ColumnStore {cs_size:,.0f} MB ({compression:.1f}x compression)")
        print()

    def save_results(self) -> Path:
        """
        Save benchmark results to CSV.

        Returns:
            Path to saved CSV file
        """
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
        filename = RESULTS_DIR / f"benchmark_{timestamp}.csv"

        df = pd.DataFrame(self.results)
        df.to_csv(filename, index=False)

        print(f"Results saved to: {filename}")
        print()

        return filename

    def cleanup(self) -> None:
        """
        Close database connections.
        """
        if self.innodb_conn:
            self.innodb_conn.close()
        if self.columnstore_conn:
            self.columnstore_conn.close()

    def run(self) -> None:
        """
        Execute the full benchmark suite.
        """
        try:
            self.setup()
            self.run_all_benchmarks()
            self.print_summary()
            self.save_results()

            print("=" * 70)
            print("Benchmark complete!")
            print("=" * 70)

        except KeyboardInterrupt:
            print("\n\nBenchmark interrupted by user")
            sys.exit(1)

        except Exception as e:
            print(f"\nBenchmark failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

        finally:
            self.cleanup()


def main():
    """Main entry point for benchmark script."""
    runner = BenchmarkRunner()
    runner.run()


if __name__ == "__main__":
    main()

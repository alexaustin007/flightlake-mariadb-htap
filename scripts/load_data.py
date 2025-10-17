"""
Data Loading Module

This script loads enriched flight route data into MariaDB tables.
Supports both InnoDB (transactional) and ColumnStore (analytical) engines
with optimized loading strategies for each.
"""

import argparse
import subprocess
import shutil
import sys
import time
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple

from config import (
    PROCESSED_DATA_DIR, INNODB_TABLE, COLUMNSTORE_TABLE,
    ETL_CONFIG
)
from db_connector import DatabaseConnection
from utils import format_time, format_number, progress_bar


class DataLoader:
    """
    Handles loading CSV data into MariaDB tables with engine-specific optimizations.
    """

    def __init__(self, csv_path: Path):
        """
        Initialize the data loader.

        Args:
            csv_path: Path to the enriched CSV file
        """
        self.csv_path = csv_path
        self.chunk_size = ETL_CONFIG['chunk_size']

        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

    def detect_cpimport(self) -> Optional[str]:
        """
        Detect if cpimport is available on the system.

        Returns:
            Path to cpimport executable or None if not found
        """
        print("Detecting cpimport availability...")

        cpimport_path = shutil.which('cpimport')

        if cpimport_path:
            print(f"  Found cpimport: {cpimport_path}")
            return cpimport_path
        else:
            print("  cpimport not found in PATH")
            common_paths = [
                '/usr/bin/cpimport',
                '/usr/local/bin/cpimport',
                '/usr/local/mariadb/columnstore/bin/cpimport'
            ]

            for path in common_paths:
                if Path(path).exists():
                    print(f"  Found cpimport: {path}")
                    return path

            print("  cpimport not available - will use SQL fallback")
            return None

    def get_row_count(self) -> int:
        """
        Get total row count from CSV (excluding header).

        Returns:
            Number of data rows in CSV
        """
        with open(self.csv_path, 'r') as f:
            return sum(1 for _ in f) - 1

    def validate_table_count(self, table_name: str, expected_count: int) -> bool:
        """
        Validate that table has expected row count.

        Args:
            table_name: Name of table to check
            expected_count: Expected number of rows

        Returns:
            True if counts match, False otherwise
        """
        with DatabaseConnection(table_name) as conn:
            result = conn.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            actual_count = result[0][0] if result else 0

            print(f"\n  Validation:")
            print(f"    Expected rows: {format_number(expected_count)}")
            print(f"    Actual rows:   {format_number(actual_count)}")

            if actual_count == expected_count:
                print(f"    Status: SUCCESS")
                return True
            else:
                print(f"    Status: MISMATCH")
                return False

    def load_innodb(self) -> Tuple[bool, float]:
        """
        Load data into InnoDB table using chunked inserts.

        Returns:
            Tuple of (success, elapsed_time)
        """
        print("=" * 70)
        print(f"Loading data into InnoDB table: {INNODB_TABLE}")
        print("=" * 70)

        start_time = time.time()

        try:
            # Get total row count
            total_rows = self.get_row_count()
            print(f"Total rows to load: {format_number(total_rows)}\n")

            # Clear existing data
            print("Clearing existing data...")
            with DatabaseConnection(INNODB_TABLE) as conn:
                deleted = conn.execute_write(f"DELETE FROM {INNODB_TABLE}")
                print(f"  Deleted {format_number(deleted)} existing rows\n")

            print(f"Loading data in chunks of {format_number(self.chunk_size)}...")

            # Read and insert in chunks
            chunk_iterator = pd.read_csv(self.csv_path, chunksize=self.chunk_size)
            rows_processed = 0

            with DatabaseConnection(INNODB_TABLE) as conn:
                for chunk_num, chunk in enumerate(chunk_iterator, 1):
                    # Replace NaN values with None for proper NULL handling
                    chunk = chunk.where(pd.notna(chunk), None)

                    # Convert DataFrame to list of tuples
                    data = [tuple(row) for row in chunk.values]

                    # Build INSERT statement
                    columns = ', '.join(chunk.columns)
                    placeholders = ', '.join(['%s'] * len(chunk.columns))
                    insert_sql = f"INSERT INTO {INNODB_TABLE} ({columns}) VALUES ({placeholders})"

                    # Execute batch insert
                    conn.executemany(insert_sql, data)

                    rows_processed += len(chunk)
                    progress = progress_bar(rows_processed, total_rows, 50)
                    print(f"  {progress} Loaded {format_number(rows_processed)} rows", end='\r')

            print(f"  {progress_bar(total_rows, total_rows, 50)} Loaded {format_number(total_rows)} rows")

            elapsed = time.time() - start_time
            print(f"\nInnoDB loading completed in {format_time(elapsed)}")

            # Validate
            success = self.validate_table_count(INNODB_TABLE, total_rows)

            return success, elapsed

        except Exception as e:
            print(f"\nError loading InnoDB data: {e}")
            return False, time.time() - start_time

    def load_columnstore_cpimport(self, cpimport_path: str) -> Tuple[bool, float]:
        """
        Load data into ColumnStore using cpimport.

        Args:
            cpimport_path: Path to cpimport executable

        Returns:
            Tuple of (success, elapsed_time)
        """
        print("=" * 70)
        print(f"Loading data into ColumnStore table: {COLUMNSTORE_TABLE}")
        print("Using cpimport (fast bulk loader)")
        print("=" * 70)

        start_time = time.time()

        try:
            # Get database connection details
            with DatabaseConnection(COLUMNSTORE_TABLE) as conn:
                db_config = conn.config

            # Clear existing data
            print("Clearing existing data...")
            with DatabaseConnection(COLUMNSTORE_TABLE) as conn:
                deleted = conn.execute_write(f"DELETE FROM {COLUMNSTORE_TABLE}")
                print(f"  Deleted {format_number(deleted)} existing rows\n")

            # Build cpimport command
            cmd = [
                cpimport_path,
                db_config['database'],
                COLUMNSTORE_TABLE,
                '-s', ',',
                '-E', '"',
                str(self.csv_path.absolute())
            ]

            print(f"Running cpimport command:")
            print(f"  {' '.join(cmd)}\n")

            # Execute cpimport
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600
            )

            if result.returncode == 0:
                print("cpimport output:")
                print(result.stdout)

                elapsed = time.time() - start_time
                print(f"\nColumnStore loading completed in {format_time(elapsed)}")

                # Validate
                total_rows = self.get_row_count()
                success = self.validate_table_count(COLUMNSTORE_TABLE, total_rows)

                return success, elapsed
            else:
                print(f"cpimport failed with return code {result.returncode}")
                print(f"Error output: {result.stderr}")
                return False, time.time() - start_time

        except subprocess.TimeoutExpired:
            print("\ncpimport timed out after 10 minutes")
            return False, time.time() - start_time
        except Exception as e:
            print(f"\nError running cpimport: {e}")
            return False, time.time() - start_time

    def load_columnstore_sql(self) -> Tuple[bool, float]:
        """
        Load data into ColumnStore using SQL (fallback method).

        Returns:
            Tuple of (success, elapsed_time)
        """
        print("=" * 70)
        print(f"Loading data into ColumnStore table: {COLUMNSTORE_TABLE}")
        print("Using SQL fallback (slower method)")
        print("=" * 70)
        print("WARNING: This may take 15-30 minutes without cpimport\n")

        start_time = time.time()

        try:
            # Get total row count
            total_rows = self.get_row_count()
            print(f"Total rows to load: {format_number(total_rows)}\n")

            # Clear existing data
            print("Clearing existing data...")
            with DatabaseConnection(COLUMNSTORE_TABLE) as conn:
                deleted = conn.execute_write(f"DELETE FROM {COLUMNSTORE_TABLE}")
                print(f"  Deleted {format_number(deleted)} existing rows\n")

            print(f"Loading data in chunks of {format_number(self.chunk_size)}...")

            # Read and insert in chunks
            chunk_iterator = pd.read_csv(self.csv_path, chunksize=self.chunk_size)
            rows_processed = 0

            with DatabaseConnection(COLUMNSTORE_TABLE) as conn:
                for chunk_num, chunk in enumerate(chunk_iterator, 1):
                    # Replace NaN values with None for proper NULL handling
                    chunk = chunk.where(pd.notna(chunk), None)

                    # Convert DataFrame to list of tuples
                    data = [tuple(row) for row in chunk.values]

                    # Build INSERT statement
                    columns = ', '.join(chunk.columns)
                    placeholders = ', '.join(['%s'] * len(chunk.columns))
                    insert_sql = f"INSERT INTO {COLUMNSTORE_TABLE} ({columns}) VALUES ({placeholders})"

                    # Execute batch insert
                    conn.executemany(insert_sql, data)

                    rows_processed += len(chunk)
                    progress = progress_bar(rows_processed, total_rows, 50)
                    elapsed_so_far = time.time() - start_time
                    print(f"  {progress} Loaded {format_number(rows_processed)} rows ({format_time(elapsed_so_far)})", end='\r')

            print(f"  {progress_bar(total_rows, total_rows, 50)} Loaded {format_number(total_rows)} rows")

            elapsed = time.time() - start_time
            print(f"\nColumnStore loading completed in {format_time(elapsed)}")

            # Validate
            success = self.validate_table_count(COLUMNSTORE_TABLE, total_rows)

            return success, elapsed

        except Exception as e:
            print(f"\nError loading ColumnStore data: {e}")
            return False, time.time() - start_time

    def load_columnstore(self) -> Tuple[bool, float]:
        """
        Load data into ColumnStore with automatic method selection.

        Returns:
            Tuple of (success, elapsed_time)
        """
        cpimport_path = self.detect_cpimport()

        if cpimport_path and ETL_CONFIG.get('use_cpimport', True):
            return self.load_columnstore_cpimport(cpimport_path)
        else:
            return self.load_columnstore_sql()


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Load enriched flight data into MariaDB tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Load into InnoDB only:
    python scripts/load_data.py --engine innodb

  Load into ColumnStore only:
    python scripts/load_data.py --engine columnstore

  Load into both tables:
    python scripts/load_data.py --engine both

  Specify custom CSV file:
    python scripts/load_data.py --engine both --file data/custom.csv
        """
    )

    parser.add_argument(
        '--engine',
        choices=['innodb', 'columnstore', 'both'],
        default='both',
        help='Target engine(s) to load data into (default: both)'
    )

    parser.add_argument(
        '--file',
        type=str,
        help='Path to CSV file (default: data/processed/routes_enriched.csv)'
    )

    return parser.parse_args()


def main():
    """
    Main entry point for data loading.
    """
    args = parse_arguments()

    # Determine CSV file path
    if args.file:
        csv_path = Path(args.file)
    else:
        csv_path = PROCESSED_DATA_DIR / 'routes_enriched.csv'

    print("\n" + "=" * 70)
    print("FlightLake Data Loading Pipeline")
    print("=" * 70)
    print(f"CSV file: {csv_path}")
    print(f"Target engine(s): {args.engine}")
    print("=" * 70)
    print()

    # Initialize loader
    try:
        loader = DataLoader(csv_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nPlease run data enrichment first:")
        print("  python scripts/data_enrichment.py")
        sys.exit(1)

    # Track results
    results = {}

    # Load InnoDB
    if args.engine in ['innodb', 'both']:
        success, elapsed = loader.load_innodb()
        results['innodb'] = {'success': success, 'time': elapsed}
        print()

    # Load ColumnStore
    if args.engine in ['columnstore', 'both']:
        success, elapsed = loader.load_columnstore()
        results['columnstore'] = {'success': success, 'time': elapsed}
        print()

    # Print summary
    print("=" * 70)
    print("Loading Summary")
    print("=" * 70)

    for engine, result in results.items():
        status = "SUCCESS" if result['success'] else "FAILED"
        print(f"{engine.upper():15} {status:10} {format_time(result['time'])}")

    print()

    # Exit with appropriate code
    all_success = all(r['success'] for r in results.values())
    if all_success:
        print("All data loaded successfully!")
        print("\nNext steps:")
        print("  1. Run benchmarks:")
        print("     python scripts/benchmark.py")
        print("  2. Launch dashboard:")
        print("     streamlit run streamlit_app/app.py")
        sys.exit(0)
    else:
        print("Some data loads failed. Please check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

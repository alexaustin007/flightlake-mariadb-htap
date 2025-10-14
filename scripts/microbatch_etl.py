"""
Micro-Batch ETL Pipeline

This script implements a micro-batch ETL process that syncs data from
InnoDB (transactional) to ColumnStore (analytical) tables. It demonstrates
a real-world HTAP scenario where operational data is periodically synced
to the analytics engine.
"""

import mariadb
import subprocess
import time
import csv
from datetime import datetime
from pathlib import Path
import sys

from config import DB_CONFIG, INNODB_TABLE, COLUMNSTORE_TABLE, ETL_CONFIG
from db_connector import DatabaseConnection


class MicroBatchETL:
    """
    Manages micro-batch ETL from InnoDB to ColumnStore.

    Extracts new/updated records from InnoDB and loads them into ColumnStore
    at regular intervals, demonstrating near-real-time analytics.
    """

    def __init__(self, batch_interval: int = None):
        """
        Initialize the ETL pipeline.

        Args:
            batch_interval: Interval in seconds between batches (default from config)
        """
        self.batch_interval = batch_interval or ETL_CONFIG['batch_interval']
        self.last_sync_time = None
        self.conn = None
        self.cursor = None
        self.batch_count = 0
        self.total_records_synced = 0

    def connect(self) -> None:
        """
        Establish database connection.
        """
        try:
            self.conn = mariadb.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            print(f"Connected to {DB_CONFIG['database']}")
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")
            sys.exit(1)

    def extract_new_records(self) -> list:
        """
        Extract records from InnoDB that have been updated since last sync.

        Returns:
            List of tuples containing record data
        """
        if self.last_sync_time:
            sql = f"""
                SELECT * FROM {INNODB_TABLE}
                WHERE updated_at > %s
                ORDER BY updated_at
            """
            self.cursor.execute(sql, (self.last_sync_time,))
        else:
            # First run - this would typically extract all records
            # For demo purposes, we'll limit to recent records
            sql = f"""
                SELECT * FROM {INNODB_TABLE}
                ORDER BY updated_at DESC
                LIMIT 1000
            """
            self.cursor.execute(sql)

        results = self.cursor.fetchall()
        return results

    def export_to_csv(self, records: list, filename: str) -> Path:
        """
        Export records to CSV for bulk loading.

        Args:
            records: List of tuples containing record data
            filename: Output filename

        Returns:
            Path to CSV file
        """
        if not records:
            return None

        csv_path = Path(ETL_CONFIG['temp_dir']) / filename

        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(records)

        return csv_path

    def load_to_columnstore_cpimport(self, csv_file: Path) -> bool:
        """
        Load CSV into ColumnStore using cpimport (fast bulk loader).

        Args:
            csv_file: Path to CSV file

        Returns:
            True if successful, False otherwise
        """
        cmd = [
            'cpimport',
            DB_CONFIG['database'],
            COLUMNSTORE_TABLE,
            '-l', str(csv_file)
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                return True
            else:
                print(f"  cpimport failed: {result.stderr}")
                return False
        except FileNotFoundError:
            return False
        except subprocess.TimeoutExpired:
            print("  cpimport timed out")
            return False

    def load_to_columnstore_sql(self, csv_file: Path) -> bool:
        """
        Fallback: Load CSV using SQL LOAD DATA INFILE.

        Args:
            csv_file: Path to CSV file

        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"""
                LOAD DATA LOCAL INFILE '{csv_file}'
                INTO TABLE {COLUMNSTORE_TABLE}
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\n'
            """
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except mariadb.Error as e:
            print(f"  SQL load failed: {e}")
            return False

    def load_to_columnstore_insert(self, records: list) -> bool:
        """
        Fallback: Load records using INSERT statements in batches.

        Args:
            records: List of tuples containing record data

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get column count from first record
            if not records:
                return True

            placeholders = ', '.join(['?'] * len(records[0]))
            sql = f"INSERT INTO {COLUMNSTORE_TABLE} VALUES ({placeholders})"

            # Process in chunks
            chunk_size = ETL_CONFIG['chunk_size']
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                self.cursor.executemany(sql, chunk)
                self.conn.commit()

            return True
        except mariadb.Error as e:
            print(f"  INSERT failed: {e}")
            self.conn.rollback()
            return False

    def run_batch(self) -> None:
        """
        Execute one batch cycle of the ETL process.
        """
        self.batch_count += 1
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Batch #{self.batch_count}")
        print("-" * 60)

        # Extract
        print("  [1/3] Extracting new records from InnoDB...")
        records = self.extract_new_records()
        print(f"        Found {len(records)} new/updated records")

        if len(records) == 0:
            print("        No new data to sync")
            return

        # Transform (minimal - data already enriched)
        # In a real scenario, you might apply business rules here
        print("  [2/3] Transforming data...")
        # No transformation needed for this demo

        # Load
        print("  [3/3] Loading to ColumnStore...")

        # Try cpimport first (fastest)
        if ETL_CONFIG['use_cpimport']:
            csv_file = self.export_to_csv(records, f'microbatch_{int(time.time())}.csv')

            if csv_file:
                print(f"        Exported to {csv_file}")
                print("        Attempting cpimport...")

                if self.load_to_columnstore_cpimport(csv_file):
                    print("        Loaded via cpimport")
                    csv_file.unlink()  # Clean up temp file
                    self.total_records_synced += len(records)
                    self.last_sync_time = datetime.now()
                    return

                # Clean up temp file if cpimport failed
                if csv_file.exists():
                    csv_file.unlink()

        # Fallback to SQL INSERT
        print("        Using SQL INSERT (slower)...")
        if self.load_to_columnstore_insert(records):
            print("        Loaded via SQL INSERT")
            self.total_records_synced += len(records)
            self.last_sync_time = datetime.now()
        else:
            print("        Load failed")

    def run_continuous(self) -> None:
        """
        Run continuous micro-batch processing.
        """
        print("=" * 60)
        print("FlightLake Micro-Batch ETL")
        print("=" * 60)
        print(f"Batch interval: {self.batch_interval} seconds")
        print(f"Source: {INNODB_TABLE}")
        print(f"Target: {COLUMNSTORE_TABLE}")
        print()
        print("Press Ctrl+C to stop")
        print("=" * 60)

        self.connect()

        try:
            while True:
                self.run_batch()

                print()
                print(f"  Total batches: {self.batch_count}")
                print(f"  Total records synced: {self.total_records_synced}")
                print(f"  Next sync in {self.batch_interval} seconds...")
                print()

                time.sleep(self.batch_interval)

        except KeyboardInterrupt:
            print("\n")
            print("=" * 60)
            print("ETL process stopped by user")
            print("=" * 60)
            print(f"Summary:")
            print(f"  Total batches: {self.batch_count}")
            print(f"  Total records synced: {self.total_records_synced}")
            print()

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

    def run_once(self) -> None:
        """
        Run a single batch (useful for testing).
        """
        print("=" * 60)
        print("FlightLake Micro-Batch ETL (Single Batch)")
        print("=" * 60)
        print()

        self.connect()

        try:
            self.run_batch()

            print()
            print("=" * 60)
            print(f"Batch complete")
            print(f"  Records synced: {self.total_records_synced}")
            print("=" * 60)

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()


def main():
    """
    Main entry point for micro-batch ETL.
    """
    import argparse

    parser = argparse.ArgumentParser(description='FlightLake Micro-Batch ETL')
    parser.add_argument(
        '--interval',
        type=int,
        default=ETL_CONFIG['batch_interval'],
        help='Batch interval in seconds'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run a single batch and exit'
    )

    args = parser.parse_args()

    etl = MicroBatchETL(batch_interval=args.interval)

    if args.once:
        etl.run_once()
    else:
        etl.run_continuous()


if __name__ == "__main__":
    main()

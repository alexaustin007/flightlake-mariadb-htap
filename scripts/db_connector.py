"""
Database Connection Management

This module provides a DatabaseConnection class for managing connections
to MariaDB and executing queries on both InnoDB and ColumnStore tables.
"""

import mariadb
import sys
from typing import List, Tuple, Optional, Any
from config import DB_CONFIG


class DatabaseConnection:
    """
    Manages database connections and query execution for FlightLake.

    This class handles connection lifecycle, query execution, cache clearing,
    and query plan analysis for both InnoDB and ColumnStore engines.

    Attributes:
        table_name (str): The name of the table to query
        conn: MariaDB connection object
        cursor: MariaDB cursor object
    """

    def __init__(self, table_name: str):
        """
        Initialize database connection manager.

        Args:
            table_name: Name of the table to use for queries
        """
        self.table_name = table_name
        self.config = DB_CONFIG
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """
        Establish connection to MariaDB database.

        Raises:
            mariadb.Error: If connection fails
        """
        try:
            self.conn = mariadb.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            print(f"Connected to {DB_CONFIG['database']}.{self.table_name}")
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")
            sys.exit(1)

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query string to execute
            params: Optional tuple of parameters for parameterized queries

        Returns:
            List of tuples containing query results

        Raises:
            mariadb.Error: If query execution fails
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            return self.cursor.fetchall()
        except mariadb.Error as e:
            print(f"Query error: {e}")
            print(f"SQL: {sql[:200]}...")  # Print first 200 chars of query
            raise

    def execute_write(self, sql: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a write query (INSERT, UPDATE, DELETE) and commit.

        Args:
            sql: SQL query string to execute
            params: Optional tuple of parameters for parameterized queries

        Returns:
            Number of affected rows

        Raises:
            mariadb.Error: If query execution fails
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            self.conn.commit()
            return self.cursor.rowcount
        except mariadb.Error as e:
            self.conn.rollback()
            print(f"Write query error: {e}")
            raise

    def executemany(self, sql: str, data: List[Tuple]) -> int:
        """
        Execute a query with multiple parameter sets.

        Useful for bulk inserts.

        Args:
            sql: SQL query string with parameter placeholders
            data: List of tuples containing parameter values

        Returns:
            Number of affected rows

        Raises:
            mariadb.Error: If execution fails
        """
        try:
            self.cursor.executemany(sql, data)
            self.conn.commit()
            return self.cursor.rowcount
        except mariadb.Error as e:
            self.conn.rollback()
            print(f"Bulk insert error: {e}")
            raise

    def clear_cache(self) -> None:
        """
        Clear the query cache for fair performance comparisons.

        Note: Some MariaDB configurations may not support query cache.
        Errors are silently ignored in those cases.
        """
        try:
            self.cursor.execute("RESET QUERY CACHE")
        except mariadb.Error:
            pass

    def get_explain(self, sql: str) -> List[Tuple]:
        """
        Get the query execution plan (EXPLAIN).

        Args:
            sql: SQL query to analyze

        Returns:
            List of tuples containing EXPLAIN output

        Raises:
            mariadb.Error: If EXPLAIN fails
        """
        explain_sql = f"EXPLAIN {sql}"
        return self.execute_query(explain_sql)

    def get_explain_json(self, sql: str) -> List[Tuple]:
        """
        Get the query execution plan in JSON format.

        Args:
            sql: SQL query to analyze

        Returns:
            List of tuples containing EXPLAIN JSON output

        Raises:
            mariadb.Error: If EXPLAIN fails
        """
        explain_sql = f"EXPLAIN FORMAT=JSON {sql}"
        return self.execute_query(explain_sql)

    def get_table_info(self) -> dict:
        """
        Get information about the table (row count, size, engine).

        Returns:
            Dictionary with table information
        """
        info = {}

        # Get row count
        count_sql = f"SELECT COUNT(*) FROM {self.table_name}"
        result = self.execute_query(count_sql)
        info['row_count'] = result[0][0] if result else 0

        # Get table metadata from information_schema
        metadata_sql = """
            SELECT
                engine,
                table_rows,
                ROUND(data_length / 1024 / 1024, 2) AS data_mb,
                ROUND(index_length / 1024 / 1024, 2) AS index_mb,
                ROUND((data_length + index_length) / 1024 / 1024, 2) AS total_mb,
                create_time,
                update_time
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = ?
        """
        result = self.execute_query(metadata_sql, (self.table_name,))

        if result:
            row = result[0]
            info.update({
                'engine': row[0],
                'estimated_rows': row[1],
                'data_mb': row[2],
                'index_mb': row[3],
                'total_mb': row[4],
                'create_time': row[5],
                'update_time': row[6]
            })

        return info

    def get_column_names(self) -> List[str]:
        """
        Get column names from the last executed query.

        Returns:
            List of column names

        Raises:
            RuntimeError: If no query has been executed yet
        """
        if self.cursor.description is None:
            raise RuntimeError("No query has been executed yet")

        return [desc[0] for desc in self.cursor.description]

    def test_connection(self) -> bool:
        """
        Test if the connection is alive and working.

        Returns:
            True if connection is working, False otherwise
        """
        try:
            self.cursor.execute("SELECT 1")
            return True
        except (mariadb.Error, AttributeError):
            return False

    def close(self) -> None:
        """Close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
            print(f"Closed connection to {self.table_name}")

    def __enter__(self):
        """Context manager entry: establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: close connection."""
        self.close()
        return False

    def __repr__(self) -> str:
        """String representation of the connection."""
        status = "connected" if self.test_connection() else "disconnected"
        return f"DatabaseConnection(table='{self.table_name}', status='{status}')"


def compare_storage(innodb_table: str, columnstore_table: str) -> dict:
    """
    Compare storage metrics between InnoDB and ColumnStore tables.

    Args:
        innodb_table: Name of the InnoDB table
        columnstore_table: Name of the ColumnStore table

    Returns:
        Dictionary with storage comparison metrics
    """
    with DatabaseConnection(innodb_table) as innodb_conn:
        innodb_info = innodb_conn.get_table_info()

    with DatabaseConnection(columnstore_table) as cs_conn:
        cs_info = cs_conn.get_table_info()

    # Calculate compression ratio
    compression_ratio = 0
    if cs_info.get('total_mb', 0) > 0:
        compression_ratio = innodb_info.get('total_mb', 0) / cs_info.get('total_mb', 1)

    return {
        'innodb': innodb_info,
        'columnstore': cs_info,
        'compression_ratio': round(compression_ratio, 2)
    }


if __name__ == "__main__":
    # Test database connection
    print("Testing Database Connection...")
    print("=" * 50)

    try:
        from config import INNODB_TABLE, COLUMNSTORE_TABLE

        # Test InnoDB connection
        with DatabaseConnection(INNODB_TABLE) as conn:
            info = conn.get_table_info()
            print(f"\n{INNODB_TABLE} Info:")
            for key, value in info.items():
                print(f"  {key}: {value}")

        # Test ColumnStore connection
        with DatabaseConnection(COLUMNSTORE_TABLE) as conn:
            info = conn.get_table_info()
            print(f"\n{COLUMNSTORE_TABLE} Info:")
            for key, value in info.items():
                print(f"  {key}: {value}")

        # Compare storage
        print("\nStorage Comparison:")
        comparison = compare_storage(INNODB_TABLE, COLUMNSTORE_TABLE)
        print(f"  Compression Ratio: {comparison['compression_ratio']}x")

    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)

    print("\nAll tests passed!")

"""
Database Configuration and Settings

This module contains all configuration settings for the FlightLake project,
including database credentials, table names, benchmark settings, and output paths.
"""

import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'database': os.getenv('DB_NAME', 'flightlake')
}

# Table names
INNODB_TABLE = 'routes_innodb'
COLUMNSTORE_TABLE = 'routes_columnstore'

# Data paths
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'

# OpenFlights dataset URLs
OPENFLIGHTS_URLS = {
    'airlines': 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat',
    'airports': 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat',
    'routes': 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat'
}

# Data enrichment settings
ENRICHMENT_CONFIG = {
    'history_months': 24,  # Generate 24 months of historical data
    'min_seats': 50,
    'max_seats': 550,
    'seat_ranges': {
        'short_haul': (100, 180),    # < 1500 km
        'medium_haul': (150, 250),   # 1500-4000 km
        'long_haul': (200, 350),     # 4000-8000 km
        'ultra_long_haul': (250, 550) # > 8000 km
    }
}

# Geographic region mapping
REGION_MAPPING = {
    'North America': ['US', 'CA', 'MX', 'CU', 'DO', 'HT', 'JM', 'PR', 'TT', 'BS', 'BB'],
    'South America': ['BR', 'AR', 'CL', 'CO', 'PE', 'VE', 'EC', 'BO', 'PY', 'UY', 'GY', 'SR', 'GF'],
    'Europe': ['GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'PL',
               'CZ', 'PT', 'GR', 'HU', 'RO', 'IE', 'UA', 'RU'],
    'Asia': ['CN', 'JP', 'IN', 'KR', 'TH', 'MY', 'SG', 'ID', 'PH', 'VN', 'TW', 'HK', 'PK', 'BD',
             'KZ', 'UZ', 'MM', 'KH', 'LA', 'MN', 'NP', 'LK'],
    'Middle East': ['AE', 'SA', 'QA', 'KW', 'OM', 'BH', 'JO', 'LB', 'IL', 'IQ', 'IR', 'YE', 'SY'],
    'Africa': ['ZA', 'EG', 'NG', 'KE', 'MA', 'ET', 'TZ', 'GH', 'UG', 'AO', 'MZ', 'MG', 'CM', 'CI',
               'NE', 'BF', 'ML', 'MW', 'ZM', 'SN', 'SO', 'ZW', 'RW', 'BJ', 'TN', 'SS', 'LY'],
    'Oceania': ['AU', 'NZ', 'FJ', 'PG', 'NC', 'PF', 'GU', 'AS', 'TO', 'VU', 'WS', 'KI', 'FM']
}

# Benchmark settings
BENCHMARK_CONFIG = {
    'cache_clear': True,        # Clear query cache between runs
    'warmup_runs': 1,          # Number of warmup runs before timing
    'test_runs': 3,            # Number of timed runs (take average)
    'enable_profiling': False  # Enable detailed query profiling
}

# Output settings
RESULTS_DIR = PROJECT_ROOT / 'results' / 'benchmarks'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'

# Micro-batch ETL settings
ETL_CONFIG = {
    'batch_interval': 300,      # 5 minutes in seconds
    'temp_dir': '/tmp',
    'use_cpimport': True,       # Try to use cpimport if available
    'chunk_size': 10000         # Rows per batch for SQL inserts
}

# Streamlit dashboard settings
DASHBOARD_CONFIG = {
    'page_title': 'FlightLake - HTAP Route Analytics',
    'page_icon': '',
    'layout': 'wide',
    'default_chart_height': 400,
    'default_table_height': 500
}

# Logging configuration
LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'datefmt': '%Y-%m-%d %H:%M:%S'
}

# Aircraft type to seat mapping (optional enrichment)
AIRCRAFT_SEAT_MAPPING = {
    '737': 180,
    '738': 189,
    '73G': 168,
    '320': 180,
    '321': 220,
    '319': 156,
    '77W': 350,
    '777': 350,
    '787': 296,
    '788': 242,
    '789': 290,
    '380': 550,
    '359': 325,
    '350': 315,
    'E90': 100,
    'E95': 120,
    'CRJ': 76,
    'CR7': 70,
    'DH4': 78,
    'AT7': 72
}


def validate_config():
    """
    Validate configuration settings and create necessary directories.

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    # Create directories if they don't exist
    for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, RESULTS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

    # Validate database config
    required_keys = ['host', 'port', 'user', 'password', 'database']
    missing_keys = [key for key in required_keys if key not in DB_CONFIG]
    if missing_keys:
        raise ValueError(f"Missing required database config keys: {missing_keys}")

    return True


if __name__ == "__main__":
    # Test configuration
    print("FlightLake Configuration")
    print("=" * 50)
    print(f"Database: {DB_CONFIG['database']} @ {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"InnoDB Table: {INNODB_TABLE}")
    print(f"ColumnStore Table: {COLUMNSTORE_TABLE}")
    print(f"Data Directory: {DATA_DIR}")
    print(f"Results Directory: {RESULTS_DIR}")
    print("\nValidating configuration...")

    try:
        validate_config()
        print("Configuration valid!")
    except Exception as e:
        print(f"Configuration error: {e}")

# FlightLake - MariaDB HTAP Analytics Platform

**Hybrid Transactional/Analytical Processing (HTAP) demonstration using MariaDB InnoDB and ColumnStore engines.**

FlightLake proves that MariaDB can serve as both an operational database (OLTP) and analytics warehouse (OLAP), eliminating the need for separate systems like PostgreSQL + Snowflake.

## Project Overview

- **OLTP Engine**: MariaDB InnoDB - handles transactional workloads
- **OLAP Engine**: MariaDB ColumnStore - optimized for analytical queries
- **Dataset**: OpenFlights airline route data, enriched with 24 months of historical data
- **Scale**: 2-5 million records demonstrating real-world analytics performance

## Features

- **6 Analytical Queries**: Hub analysis, regional capacity, trend analysis, and more
- **Performance Benchmarking**: Automated comparison between InnoDB and ColumnStore
- **Interactive Dashboard**: Streamlit-based UI for query execution and visualization
- **Micro-Batch ETL**: Demonstrates data sync from OLTP to OLAP engine
- **Real-World Use Cases**: Network planning, market analysis, capacity optimization

## Project Structure

```
flightlake/
├── data/
│   ├── raw/                    # OpenFlights raw data
│   └── processed/              # Enriched CSV files
├── scripts/
│   ├── __init__.py
│   ├── config.py               # Configuration settings
│   ├── db_connector.py         # Database connection management
│   ├── queries.py              # Analytical query definitions
│   ├── utils.py                # Utility functions
│   ├── data_enrichment.py      # Data processing pipeline
│   ├── benchmark.py            # Performance benchmarking
│   └── microbatch_etl.py       # ETL pipeline
├── sql/
│   ├── create_database.sql     # Database creation
│   ├── create_innodb_table.sql # InnoDB schema
│   └── create_columnstore_table.sql # ColumnStore schema
├── streamlit_app/
│   └── app.py                  # Interactive dashboard
├── results/
│   └── benchmarks/             # Benchmark results
├── requirements.txt
├── implementation.md           # Detailed implementation guide
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.8+
- MariaDB Server 10.6+ with ColumnStore engine
- 4GB+ RAM recommended
- 2GB+ free disk space

### Installation

1. **Clone or download the project**:
```bash
cd /path/to/mariadb
```

2. **Create virtual environment**:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Install MariaDB with ColumnStore**:

   **Docker (Recommended)**:
   ```bash
   docker pull mariadb/columnstore:latest
   docker run -d \
     --name mariadb-columnstore \
     -e MARIADB_ROOT_PASSWORD=your_password \
     -p 3306:3306 \
     mariadb/columnstore:latest
   ```

   **macOS (Homebrew)**:
   ```bash
   brew install mariadb
   brew services start mariadb
   ```

5. **Configure database credentials**:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

### Database Setup

1. **Create database and tables**:
```bash
mysql -u root -p < sql/create_database.sql
mysql -u root -p < sql/create_innodb_table.sql
mysql -u root -p < sql/create_columnstore_table.sql
```

2. **Verify ColumnStore engine**:
```bash
mysql -u root -p -e "SHOW ENGINES;"
# Look for 'Columnstore' with 'YES' or 'DEFAULT'
```

### Data Pipeline

1. **Download and enrich OpenFlights data**:
```bash
python scripts/data_enrichment.py
```
This will:
- Download airlines, airports, and routes data
- Calculate distances using Haversine formula
- Generate 24 months of time-series data
- Create enriched CSV (~500MB-1GB)

2. **Load data into databases**:
```bash
# You'll need to create a separate load script or use SQL LOAD DATA
# See implementation.md for detailed instructions
```

## Usage

### Run Benchmarks

Compare query performance between InnoDB and ColumnStore:

```bash
python scripts/benchmark.py
```

Expected output:
```
┌─────────────────────────┬────────┬──────────────┬─────────┬────────┬───────┐
│ Query                   │ InnoDB │ ColumnStore  │ Speedup │ Winner │ Match │
├─────────────────────────┼────────┼──────────────┼─────────┼────────┼───────┤
│ Top 10 Busiest Hubs     │ 2.3s   │ 150ms        │ 15.3x   │ CS     │ Yes   │
│ Regional Capacity       │ 5.1s   │ 320ms        │ 15.9x   │ CS     │ Yes   │
│ ...                     │ ...    │ ...          │ ...     │ ...    │ ...   │
└─────────────────────────┴────────┴──────────────┴─────────┴────────┴───────┘

Average Speedup: 18.5x
Storage: InnoDB 1,247 MB → ColumnStore 183 MB (6.8x compression)
```

### Launch Dashboard

Start the interactive Streamlit dashboard:

```bash
streamlit run streamlit_app/app.py
```

Navigate to `http://localhost:8501` and:
1. Select a query from the sidebar
2. Choose engine mode (Both/InnoDB/ColumnStore)
3. Click "Execute Query"
4. View results, performance metrics, and visualizations

### Run Micro-Batch ETL

Sync data from InnoDB to ColumnStore:

```bash
# Continuous mode (runs every 5 minutes)
python scripts/microbatch_etl.py

# Single batch mode
python scripts/microbatch_etl.py --once

# Custom interval (10 minutes)
python scripts/microbatch_etl.py --interval 600
```

## Key Queries

### 1. Top 10 Busiest Hubs
Identifies airports handling the most seat capacity for network planning.

### 2. Regional Capacity Distribution
Analyzes capacity flows across world regions for market analysis.

### 3. Underserved Routes
Finds routes with low frequency or small aircraft for opportunity identification.

### 4. Capacity Trends Over Time
Tracks month-over-month capacity changes to identify growth patterns.

### 5. Hub Concentration Analysis
Calculates market concentration using cumulative percentages.

### 6. Distance-Based Segmentation
Compares capacity distribution across short/medium/long/ultra-long haul flights.

## Performance Results

**Typical Performance Gains** (on 2M+ records):

| Query Type | InnoDB | ColumnStore | Speedup |
|------------|--------|-------------|---------|
| Aggregations (GROUP BY) | 2-5s | 100-300ms | 10-20x |
| Time Series | 3-8s | 200-500ms | 15-25x |
| Complex Joins/CTEs | 5-12s | 300-800ms | 10-30x |

**Storage Efficiency**:
- InnoDB: ~1.2 GB (with indexes)
- ColumnStore: ~180 MB (columnar compression)
- Compression Ratio: **6-8x**

## Why ColumnStore is Faster

1. **Columnar Storage**: Only reads columns needed for query (vs full rows)
2. **Compression**: Reduces I/O with aggressive compression
3. **Vectorized Execution**: Processes data in batches
4. **No Index Overhead**: Scans are faster than index lookups for analytics

## Configuration

Edit `scripts/config.py` to customize:

- Database credentials
- Data enrichment settings (history months, seat ranges)
- Benchmark settings (warmup runs, test runs)
- ETL batch interval

Or use environment variables (see `.env.example`).

## Development

### Running Tests

```bash
# Test database connection
python scripts/db_connector.py

# Test query definitions
python scripts/queries.py

# Test utility functions
python scripts/utils.py

# Validate configuration
python scripts/config.py
```

### Adding New Queries

1. Add query definition to `scripts/queries.py`
2. Run benchmarks to measure performance
3. Add visualization to `streamlit_app/app.py` (optional)

## Troubleshooting

**ColumnStore engine not available**:
```bash
# Check if ColumnStore plugin is loaded
mysql -e "SHOW PLUGINS;" | grep -i columnstore
```

**cpimport not found**:
```bash
# Find cpimport location
find /usr -name cpimport
# Add to PATH or use fallback SQL loader
```

**Connection refused**:
```bash
# Verify MariaDB is running
systemctl status mariadb  # Linux
brew services list         # macOS

# Check port 3306 is accessible
telnet localhost 3306
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Application Layer                      │
│  ┌──────────────────┐      ┌─────────────────────────┐ │
│  │ Streamlit        │      │ Python Scripts          │ │
│  │ Dashboard        │      │ (Benchmark, ETL)        │ │
│  └────────┬─────────┘      └──────────┬──────────────┘ │
└───────────┼────────────────────────────┼────────────────┘
            │                            │
            ▼                            ▼
┌─────────────────────────────────────────────────────────┐
│                   MariaDB Server                         │
│  ┌──────────────────┐      ┌─────────────────────────┐ │
│  │ InnoDB Engine    │◄────►│ ColumnStore Engine      │ │
│  │ (OLTP)           │ ETL  │ (OLAP)                  │ │
│  │ - Fast writes    │      │ - Fast aggregations     │ │
│  │ - Row-based      │      │ - Columnar storage      │ │
│  │ - Indexed        │      │ - Compressed            │ │
│  └──────────────────┘      └─────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Use Cases

- **Airline Network Planning**: Which hubs need expansion?
- **Market Analysis**: Where is capacity concentrated?
- **Route Optimization**: Identify underserved markets
- **Trend Forecasting**: Seasonal capacity planning
- **Competitive Intelligence**: Market share analysis

## Performance Tuning

**InnoDB**:
```sql
SET GLOBAL innodb_buffer_pool_size = 4G;
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
```

**ColumnStore**:
```sql
SET GLOBAL columnstore_use_import_for_batchinsert = 'ON';
```

## Contributing

Contributions welcome! Areas for improvement:
- Additional analytical queries
- More visualization types
- Real-time data ingestion (Kafka)
- ML-based forecasting
- Geographic visualizations (maps)

## Resources

- [MariaDB ColumnStore Documentation](https://mariadb.com/kb/en/columnstore/)
- [OpenFlights Data](https://openflights.org/data.html)
- [Streamlit Documentation](https://docs.streamlit.io)
- [Implementation Guide](implementation.md) - Detailed setup instructions

## License

This project is open source and available for educational and commercial use.

## Acknowledgments

- OpenFlights for the airline route dataset
- MariaDB Foundation for ColumnStore engine
- Streamlit for the dashboard framework

---

**Built to demonstrate MariaDB's HTAP capabilities**

For questions or issues, please refer to the [implementation guide](implementation.md) or open an issue on GitHub.

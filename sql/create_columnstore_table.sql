-- FlightLake ColumnStore Table Creation Script serves as the analytical data store (OLAP)

USE flightlake;

-- Drop table if exists (for clean re-creation)
DROP TABLE IF EXISTS routes_columnstore;

-- Create ColumnStore table
CREATE TABLE routes_columnstore (
    route_id BIGINT,

    -- Flight identifiers
    airline_code VARCHAR(3),
    airline_name VARCHAR(100),
    flight_number VARCHAR(10),

    -- Origin airport details
    origin_airport VARCHAR(4),
    origin_city VARCHAR(100),
    origin_country VARCHAR(100),
    origin_region VARCHAR(50),
    origin_latitude DECIMAL(10, 6),
    origin_longitude DECIMAL(10, 6),

    -- Destination airport details
    destination_airport VARCHAR(4),
    destination_city VARCHAR(100),
    destination_country VARCHAR(100),
    destination_region VARCHAR(50),
    destination_latitude DECIMAL(10, 6),
    destination_longitude DECIMAL(10, 6),

    -- Flight metrics
    distance_km DECIMAL(10, 2),
    seats INT,
    aircraft_type VARCHAR(50),

    -- Time dimension
    flight_date DATE,
    flight_year INT,
    flight_month INT,
    flight_quarter INT,

    -- Metadata
    codeshare BOOLEAN,
    stops INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) ENGINE=ColumnStore
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Analytical route data (OLAP) - optimized for aggregations';

-- Note: ColumnStore doesn't use traditional indexes
-- Columnar storage provides automatic optimization for analytical queries

-- Display table info
SHOW CREATE TABLE routes_columnstore;

SELECT 'ColumnStore table created successfully!' AS Status;

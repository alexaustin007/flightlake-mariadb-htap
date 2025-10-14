-- FlightLake InnoDB Table Creation Script
-- This table serves as the transactional data store (OLTP)

USE flightlake;

-- Drop table if exists (for clean re-creation)
DROP TABLE IF EXISTS routes_innodb;

-- Create InnoDB table
CREATE TABLE routes_innodb (
    route_id BIGINT AUTO_INCREMENT PRIMARY KEY,

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
    codeshare BOOLEAN DEFAULT FALSE,
    stops INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indexes for common transactional queries
    INDEX idx_date (flight_date),
    INDEX idx_origin (origin_airport),
    INDEX idx_destination (destination_airport),
    INDEX idx_region (origin_region),
    INDEX idx_airline (airline_code),
    INDEX idx_year_month (flight_year, flight_month),
    INDEX idx_updated_at (updated_at)  -- For micro-batch ETL
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Transactional route data (OLTP)';

-- Display table info
SHOW CREATE TABLE routes_innodb;

SELECT 'InnoDB table created successfully!' AS Status;

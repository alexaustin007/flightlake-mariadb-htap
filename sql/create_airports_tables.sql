-- FlightLake Airports Tables Creation Script
-- Creates both InnoDB (OLTP) and ColumnStore (OLAP) versions

USE flightlake;

-- ============================================
-- AIRPORTS INNODB TABLE (Transactional)
-- ============================================

DROP TABLE IF EXISTS airports_innodb;

CREATE TABLE airports_innodb (
    airport_id INT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    iata_code VARCHAR(3),
    icao_code VARCHAR(4),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    altitude INT,
    timezone DECIMAL(4, 2),
    dst CHAR(1),
    tz_database VARCHAR(100),
    type VARCHAR(50),
    source VARCHAR(50),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indexes for transactional queries
    INDEX idx_iata_code (iata_code),
    INDEX idx_icao_code (icao_code),
    INDEX idx_city (city),
    INDEX idx_country (country),
    INDEX idx_type (type),
    INDEX idx_location (latitude, longitude)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Airports data - InnoDB for transactional operations';

SELECT 'airports_innodb table created successfully!' AS Status;


-- ============================================
-- AIRPORTS COLUMNSTORE TABLE (Analytical)
-- ============================================

DROP TABLE IF EXISTS airports_columnstore;

CREATE TABLE airports_columnstore (
    airport_id INT,
    name VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100),
    iata_code VARCHAR(3),
    icao_code VARCHAR(4),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    altitude INT,
    timezone DECIMAL(4, 2),
    dst CHAR(1),
    tz_database VARCHAR(100),
    type VARCHAR(50),
    source VARCHAR(50),

    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) ENGINE=ColumnStore
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Airports data - ColumnStore for analytical queries';

SELECT 'airports_columnstore table created successfully!' AS Status;

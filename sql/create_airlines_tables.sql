-- FlightLake Airlines Tables Creation Script
-- Creates both InnoDB (OLTP) and ColumnStore (OLAP) versions

USE flightlake;


-- AIRLINES INNODB TABLE (Transactional)


DROP TABLE IF EXISTS airlines_innodb;

CREATE TABLE airlines_innodb (
    airline_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    alias VARCHAR(100),
    iata_code VARCHAR(3),
    icao_code VARCHAR(4),
    callsign VARCHAR(100),
    country VARCHAR(100),
    active CHAR(1) DEFAULT 'Y',

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indexes for transactional queries
    INDEX idx_iata_code (iata_code),
    INDEX idx_icao_code (icao_code),
    INDEX idx_country (country),
    INDEX idx_active (active)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Airlines data - InnoDB for transactional operations';

SELECT 'airlines_innodb table created successfully!' AS Status;



-- AIRLINES COLUMNSTORE TABLE (Analytical)


DROP TABLE IF EXISTS airlines_columnstore;

CREATE TABLE airlines_columnstore (
    airline_id INT,
    name VARCHAR(100),
    alias VARCHAR(100),
    iata_code VARCHAR(3),
    icao_code VARCHAR(4),
    callsign VARCHAR(100),
    country VARCHAR(100),
    active CHAR(1),

    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) ENGINE=ColumnStore
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Airlines data - ColumnStore for analytical queries';

SELECT 'airlines_columnstore table created successfully!' AS Status;

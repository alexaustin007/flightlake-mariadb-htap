-- FlightLake Routes Tables Creation Script
-- Creates both InnoDB (OLTP) and ColumnStore (OLAP) versions
-- NOTE: These are for RAW routes data from OpenFlights

USE flightlake;


-- ROUTES INNODB TABLE (Transactional)


DROP TABLE IF EXISTS routes_innodb;

CREATE TABLE routes_innodb (
    route_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    airline_code VARCHAR(3),
    airline_id INT,
    origin_airport VARCHAR(4),
    origin_airport_id INT,
    destination_airport VARCHAR(4),
    destination_airport_id INT,
    codeshare CHAR(1),
    stops INT DEFAULT 0,
    equipment VARCHAR(200),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indexes for transactional queries
    INDEX idx_airline_code (airline_code),
    INDEX idx_airline_id (airline_id),
    INDEX idx_origin_airport (origin_airport),
    INDEX idx_destination_airport (destination_airport),
    INDEX idx_origin_airport_id (origin_airport_id),
    INDEX idx_destination_airport_id (destination_airport_id),
    INDEX idx_stops (stops)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Routes data - InnoDB for transactional operations';

SELECT 'routes_innodb table created successfully!' AS Status;



-- ROUTES COLUMNSTORE TABLE (Analytical)


DROP TABLE IF EXISTS routes_columnstore;

CREATE TABLE routes_columnstore (
    route_id BIGINT,
    airline_code VARCHAR(3),
    airline_id INT,
    origin_airport VARCHAR(4),
    origin_airport_id INT,
    destination_airport VARCHAR(4),
    destination_airport_id INT,
    codeshare CHAR(1),
    stops INT,
    equipment VARCHAR(200),

    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) ENGINE=ColumnStore
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Routes data - ColumnStore for analytical queries';

SELECT 'routes_columnstore table created successfully!' AS Status;

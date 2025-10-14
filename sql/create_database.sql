-- FlightLake Database Creation Script
-- This script creates the database and sets up initial configuration

-- Create database
CREATE DATABASE IF NOT EXISTS flightlake
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Use the database
USE flightlake;

-- Display confirmation
SELECT 'Database flightlake created successfully!' AS Status;

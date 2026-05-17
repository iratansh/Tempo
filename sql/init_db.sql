-- Initialize Tempo Data Warehouse
-- This script runs automatically when PostgreSQL container starts

-- Create Airflow user and database (for metadata)
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Connect to airflow database and grant schema permissions
\c airflow;
GRANT ALL ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;

-- Create Tempo data warehouse database
CREATE DATABASE tempo_warehouse;

-- Connect to tempo_warehouse
\c tempo_warehouse;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create raw data table for JSON ingestion
CREATE TABLE IF NOT EXISTS raw_data.listening_history (
    id SERIAL PRIMARY KEY,
    extraction_timestamp TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(extraction_timestamp)
);

-- Create index on extraction timestamp for faster queries
CREATE INDEX IF NOT EXISTS idx_listening_history_extraction_ts
ON raw_data.listening_history(extraction_timestamp);

-- Create index on JSONB data for faster JSON queries
CREATE INDEX IF NOT EXISTS idx_listening_history_jsonb
ON raw_data.listening_history USING GIN(raw_data);

-- Grant permissions (for dbt later)
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO postgres;

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'Tempo Data Warehouse initialized successfully!';
END $$;

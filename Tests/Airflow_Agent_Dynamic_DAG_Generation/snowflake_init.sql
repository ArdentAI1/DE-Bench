-- Initialize Snowflake tenant warehouse with a placeholder table
-- This ensures the schema is visible during connector discovery

CREATE TABLE IF NOT EXISTS _airflow_ready (
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    status VARCHAR(50) DEFAULT 'ready'
);

-- Insert a marker row
INSERT INTO _airflow_ready (status) VALUES ('ready');


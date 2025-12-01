-- Initialize Snowflake data lake with a placeholder table
-- This ensures the schema is visible during connector discovery

CREATE TABLE IF NOT EXISTS _schema_ready (
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    status VARCHAR(50) DEFAULT 'ready'
);

-- Insert a marker row
INSERT INTO _schema_ready (status) VALUES ('ready');


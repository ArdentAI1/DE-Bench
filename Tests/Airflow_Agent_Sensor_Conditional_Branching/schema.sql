-- Create tables for sensor testing
CREATE TABLE etl_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    status VARCHAR(50),
    row_count INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE processing_log (
    id SERIAL PRIMARY KEY,
    processing_type VARCHAR(50),
    file_name VARCHAR(255),
    row_count INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial data
INSERT INTO etl_jobs (job_name, status, row_count) VALUES
('daily_load', 'pending', 0),
('vendor_import', 'pending', 0);

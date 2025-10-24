-- Customer dimension table for streaming music platform
CREATE TABLE dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,  -- Natural key from source systems
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    subscription_tier VARCHAR(20) NOT NULL CHECK (subscription_tier IN ('Free', 'Premium', 'Enterprise')),
    registration_date DATE NOT NULL,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50) DEFAULT 'ETL_PIPELINE',
    UNIQUE (email)  -- Business rule: one account per email
);

-- Staging table for incoming data (typical ETL pattern)
CREATE TABLE staging_customers (
    customer_id VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    subscription_tier VARCHAR(20) NOT NULL,
    registration_date DATE NOT NULL,
    batch_id VARCHAR(50) DEFAULT NULL,  -- For tracking batch processing
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit table to track all upsert operations (production pattern)
CREATE TABLE customer_audit_log (
    audit_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE')),
    old_values JSONB,  -- Store previous state for updates
    new_values JSONB,  -- Store new state
    batch_id VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient lookups
CREATE INDEX idx_dim_customers_email ON dim_customers(email);
CREATE INDEX idx_audit_log_customer_id ON customer_audit_log(customer_id);
CREATE INDEX idx_staging_batch_id ON staging_customers(batch_id);

-- Initial seed data to simulate existing customers
INSERT INTO dim_customers (customer_id, email, first_name, last_name, subscription_tier, registration_date) VALUES
('CUST_001', 'existing@example.com', 'Existing', 'User', 'Premium', '2023-01-15'),
('CUST_002', 'legacy@example.com', 'Legacy', 'Customer', 'Free', '2022-06-20'),
-- Test customers that will be used in validation
('ALICE_001', 'alice@example.com', 'Alice', 'Johnson', 'Premium', '2023-03-10'),
('BOB_001', 'bob@example.com', 'Bob', 'Smith', 'Free', '2023-04-15'),
('CAROL_001', 'carol@example.com', 'Carol', 'Davis', 'Premium', '2023-05-20');

-- Tenant pipeline configuration table

CREATE TABLE tenant_pipeline_configs (
    tenant_id SERIAL PRIMARY KEY,
    tenant_name VARCHAR(100) UNIQUE NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    source_connection_info JSONB,
    transformation_rules JSONB,
    schedule VARCHAR(100),
    enabled BOOLEAN DEFAULT TRUE,
    snowflake_schema VARCHAR(100),
    notification_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 5 sample tenant configurations
INSERT INTO tenant_pipeline_configs 
(tenant_name, source_type, source_connection_info, transformation_rules, schedule, enabled, snowflake_schema, notification_email) 
VALUES
(
    'acme_corp',
    'postgres',
    '{"host": "db.acme.com", "port": 5432, "database": "sales_db", "table": "transactions"}'::jsonb,
    '{"columns": ["customer_id", "amount", "date"], "aggregation": "daily"}'::jsonb,
    '@hourly',
    TRUE,
    'TENANT_ACME_CORP',
    'data@acme.com'
),
(
    'beta_inc',
    'mysql',
    '{"host": "mysql.beta.com", "port": 3306, "database": "analytics", "table": "events"}'::jsonb,
    '{"filter": "WHERE event_type = ''purchase''", "transform": "UPPER(customer_email)"}'::jsonb,
    '@daily',
    TRUE,
    'TENANT_BETA_INC',
    'engineering@beta.com'
),
(
    'gamma_solutions',
    's3',
    '{"bucket": "gamma-data", "prefix": "raw/", "file_pattern": "*.csv"}'::jsonb,
    '{"csv_delimiter": ",", "skip_header": true, "columns": ["id", "value", "timestamp"]}'::jsonb,
    '@daily',
    TRUE,
    'TENANT_GAMMA_SOLUTIONS',
    'devops@gamma.com'
),
(
    'delta_systems',
    'api',
    '{"url": "https://api.delta.com/data", "auth_type": "bearer", "endpoint": "/metrics"}'::jsonb,
    '{"json_path": "$.results[*]", "flatten": true}'::jsonb,
    '@daily',
    TRUE,
    'TENANT_DELTA_SYSTEMS',
    'data-team@delta.com'
),
(
    'epsilon_disabled',
    'postgres',
    '{"host": "db.epsilon.com", "port": 5432, "database": "metrics", "table": "stats"}'::jsonb,
    '{"columns": ["metric_name", "value"], "aggregation": "none"}'::jsonb,
    '@hourly',
    FALSE,
    'TENANT_EPSILON_DISABLED',
    'admin@epsilon.com'
);

-- Create metadata tracking table
CREATE TABLE pipeline_execution_log (
    id SERIAL PRIMARY KEY,
    tenant_id INTEGER REFERENCES tenant_pipeline_configs(tenant_id),
    execution_date TIMESTAMP,
    status VARCHAR(50),
    rows_processed INTEGER,
    execution_time_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

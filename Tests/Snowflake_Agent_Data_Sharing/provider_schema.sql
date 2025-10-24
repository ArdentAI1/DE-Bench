-- Provider database schema for Data Sharing test

CREATE TABLE campaign_performance (
    campaign_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    date DATE NOT NULL,
    impressions INTEGER,
    clicks INTEGER,
    conversions INTEGER,
    spend DECIMAL(10,2)
);

CREATE TABLE customer_segments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    segment_name VARCHAR(100),
    ltv_score DECIMAL(10,2),
    churn_risk VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE product_usage_metrics (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    feature_name VARCHAR(255),
    usage_count INTEGER,
    last_used TIMESTAMP,
    date DATE
);

-- Insert data for multiple customers (Customer 1, 2, 3)
INSERT INTO campaign_performance (campaign_id, customer_id, date, impressions, clicks, conversions, spend) VALUES
(1, 1, '2024-11-01', 10000, 250, 25, 500.00),
(2, 1, '2024-11-02', 12000, 300, 30, 600.00),
(3, 2, '2024-11-01', 8000, 180, 18, 400.00),
(4, 2, '2024-11-02', 9000, 200, 20, 450.00),
(5, 3, '2024-11-01', 15000, 400, 40, 800.00);

INSERT INTO customer_segments (customer_id, segment_name, ltv_score, churn_risk) VALUES
(1, 'Enterprise', 50000.00, 'low'),
(2, 'Mid-Market', 15000.00, 'medium'),
(3, 'Enterprise', 75000.00, 'low');

INSERT INTO product_usage_metrics (customer_id, feature_name, usage_count, last_used, date) VALUES
(1, 'Dashboard', 150, '2024-11-15 10:30:00', '2024-11-15'),
(1, 'Reports', 45, '2024-11-15 11:00:00', '2024-11-15'),
(2, 'Dashboard', 80, '2024-11-15 09:00:00', '2024-11-15'),
(2, 'API', 120, '2024-11-15 10:00:00', '2024-11-15'),
(3, 'Dashboard', 200, '2024-11-15 08:00:00', '2024-11-15'),
(3, 'Reports', 90, '2024-11-15 09:30:00', '2024-11-15');

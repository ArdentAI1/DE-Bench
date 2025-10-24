-- PostgreSQL CRM database schema

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    segment VARCHAR(50),
    lifetime_value DECIMAL(10,2),
    registration_date DATE,
    region VARCHAR(50)
);

CREATE TABLE customer_preferences (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    preferred_category VARCHAR(100),
    notification_opt_in BOOLEAN DEFAULT TRUE,
    language VARCHAR(10),
    currency VARCHAR(10)
);

CREATE TABLE customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(100) UNIQUE,
    min_ltv DECIMAL(10,2),
    max_ltv DECIMAL(10,2),
    tier VARCHAR(50)
);

-- Insert customer segments
INSERT INTO customer_segments (segment_name, min_ltv, max_ltv, tier) VALUES
('Bronze', 0, 1000, 'low'),
('Silver', 1000, 5000, 'medium'),
('Gold', 5000, 20000, 'high'),
('Platinum', 20000, 999999, 'premium');

-- Insert customers
INSERT INTO customers (email, name, segment, lifetime_value, registration_date, region) VALUES
('alice@example.com', 'Alice Johnson', 'Gold', 8500.00, '2024-01-15', 'US'),
('bob@example.com', 'Bob Smith', 'Silver', 3200.00, '2024-02-01', 'US'),
('charlie@example.com', 'Charlie Brown', 'Platinum', 25000.00, '2024-01-20', 'EU'),
('diana@example.com', 'Diana Prince', 'Bronze', 450.00, '2024-03-10', 'US'),
('eve@example.com', 'Eve Davis', 'Gold', 12000.00, '2024-02-15', 'ASIA'),
('frank@example.com', 'Frank Miller', 'Silver', 2800.00, '2024-03-01', 'EU');

-- Insert customer preferences
INSERT INTO customer_preferences (customer_id, preferred_category, language, currency) VALUES
(1, 'Electronics', 'en', 'USD'),
(2, 'Clothing', 'en', 'USD'),
(3, 'Home & Kitchen', 'de', 'EUR'),
(4, 'Books', 'en', 'USD'),
(5, 'Electronics', 'ja', 'JPY'),
(6, 'Sports', 'fr', 'EUR');

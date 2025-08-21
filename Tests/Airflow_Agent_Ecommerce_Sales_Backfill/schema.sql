-- E-commerce Sales Backfill Database Schema
-- This schema supports a realistic e-commerce sales backfill scenario

-- Raw orders table (simulates source system data)
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table (order line items)
CREATE TABLE IF NOT EXISTS order_items (
    item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Customers table (for customer segmentation)
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_email VARCHAR(255),
    customer_segment VARCHAR(50) DEFAULT 'regular',
    registration_date DATE,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily sales metrics table (target for backfill aggregation)
CREATE TABLE IF NOT EXISTS daily_sales_metrics (
    metric_date DATE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    avg_order_value DECIMAL(10,2) DEFAULT 0.00,
    unique_customers INTEGER DEFAULT 0,
    top_category VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date)
);

-- Category sales breakdown (detailed metrics)
CREATE TABLE IF NOT EXISTS category_sales_daily (
    metric_date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    total_quantity INTEGER DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, category)
);

-- Customer segment metrics
CREATE TABLE IF NOT EXISTS customer_segment_daily (
    metric_date DATE NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    unique_customers INTEGER DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, customer_segment)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_category ON order_items(category);
CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(customer_segment);

-- Sample data for testing (simulates historical orders that need backfilling)
INSERT INTO customers (customer_id, customer_email, customer_segment, registration_date, country) VALUES
('CUST001', 'john@example.com', 'premium', '2024-01-01', 'USA'),
('CUST002', 'jane@example.com', 'regular', '2024-01-02', 'Canada'),
('CUST003', 'bob@example.com', 'premium', '2024-01-03', 'UK'),
('CUST004', 'alice@example.com', 'regular', '2024-01-04', 'USA'),
('CUST005', 'charlie@example.com', 'vip', '2024-01-05', 'Germany')
ON CONFLICT (customer_id) DO NOTHING;

-- Sample orders (these represent the "missing" data that needs backfilling)
INSERT INTO orders (order_id, customer_id, order_date, order_status, total_amount) VALUES
('ORD001', 'CUST001', '2024-01-15', 'completed', 150.00),
('ORD002', 'CUST002', '2024-01-15', 'completed', 89.99),
('ORD003', 'CUST003', '2024-01-16', 'completed', 299.99),
('ORD004', 'CUST001', '2024-01-16', 'completed', 75.50),
('ORD005', 'CUST004', '2024-01-17', 'completed', 199.99),
('ORD006', 'CUST005', '2024-01-17', 'completed', 450.00),
('ORD007', 'CUST002', '2024-01-18', 'completed', 125.00),
('ORD008', 'CUST003', '2024-01-18', 'completed', 89.99),
('ORD009', 'CUST001', '2024-01-19', 'completed', 320.00),
('ORD010', 'CUST004', '2024-01-19', 'completed', 95.00)
ON CONFLICT (order_id) DO NOTHING;

-- Sample order items
INSERT INTO order_items (item_id, order_id, product_id, product_name, category, quantity, unit_price, total_price) VALUES
('ITEM001', 'ORD001', 'PROD001', 'Wireless Headphones', 'Electronics', 1, 150.00, 150.00),
('ITEM002', 'ORD002', 'PROD002', 'Coffee Mug', 'Home & Garden', 2, 44.99, 89.98),
('ITEM003', 'ORD003', 'PROD003', 'Running Shoes', 'Sports', 1, 299.99, 299.99),
('ITEM004', 'ORD004', 'PROD004', 'Book Set', 'Books', 3, 25.17, 75.51),
('ITEM005', 'ORD005', 'PROD005', 'Laptop Stand', 'Electronics', 1, 199.99, 199.99),
('ITEM006', 'ORD006', 'PROD006', 'Premium Watch', 'Accessories', 1, 450.00, 450.00),
('ITEM007', 'ORD007', 'PROD007', 'Kitchen Knife Set', 'Home & Garden', 1, 125.00, 125.00),
('ITEM008', 'ORD008', 'PROD008', 'Yoga Mat', 'Sports', 1, 89.99, 89.99),
('ITEM009', 'ORD009', 'PROD009', 'Gaming Mouse', 'Electronics', 2, 160.00, 320.00),
('ITEM010', 'ORD010', 'PROD010', 'Travel Backpack', 'Accessories', 1, 95.00, 95.00)
ON CONFLICT (item_id) DO NOTHING;

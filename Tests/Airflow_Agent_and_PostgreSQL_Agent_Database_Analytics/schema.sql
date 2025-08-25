-- Create raw sales data table (simulating messy real-world data)
CREATE TABLE raw_sales_data (
    record_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    customer_email VARCHAR(255),
    customer_name VARCHAR(255),
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2) DEFAULT 0,
    order_date DATE,
    shipping_cost DECIMAL(8,2) DEFAULT 0,
    tax_rate DECIMAL(5,4) DEFAULT 0.0875,
    payment_status VARCHAR(50),
    region VARCHAR(100),
    sales_rep VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customer segments reference table
CREATE TABLE customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL,
    min_orders INTEGER,
    min_revenue DECIMAL(10,2),
    description TEXT
);

-- Insert sample sales data with various data quality issues
INSERT INTO raw_sales_data (order_id, customer_email, customer_name, product_sku, product_name, category, quantity, unit_price, discount_percent, order_date, shipping_cost, payment_status, region, sales_rep) VALUES
-- High-value customers
('ORD-001', 'john.doe@enterprise.com', 'John Doe', 'LAPTOP-PRO-001', 'Laptop Pro 15"', 'Electronics', 2, 1299.99, 10.0, '2024-01-15', 25.99, 'PAID', 'North', 'Alice Johnson'),
('ORD-002', 'john.doe@enterprise.com', 'John Doe', 'MONITOR-001', '4K Monitor', 'Electronics', 1, 599.99, 5.0, '2024-01-20', 15.99, 'PAID', 'North', 'Alice Johnson'),
('ORD-003', 'sarah.johnson@corp.com', 'Sarah Johnson', 'CHAIR-EXEC-001', 'Executive Chair', 'Furniture', 3, 399.99, 15.0, '2024-01-18', 45.00, 'PAID', 'South', 'Bob Wilson'),
-- Medium-value customers  
('ORD-004', 'mike.brown@startup.com', 'Mike Brown', 'DESK-STAND-001', 'Standing Desk', 'Furniture', 1, 299.99, 0.0, '2024-01-22', 35.00, 'PAID', 'West', 'Carol Davis'),
('ORD-005', 'mike.brown@startup.com', 'Mike Brown', 'MOUSE-001', 'Wireless Mouse', 'Electronics', 2, 29.99, 0.0, '2024-01-25', 5.99, 'PAID', 'West', 'Carol Davis'),
('ORD-006', 'emily.davis@design.com', 'Emily Davis', 'TABLET-001', 'Design Tablet', 'Electronics', 1, 499.99, 20.0, '2024-01-28', 12.99, 'PAID', 'East', 'David Miller'),
-- Low-value customers
('ORD-007', 'david.wilson@home.com', 'David Wilson', 'MUG-001', 'Coffee Mug', 'Office', 5, 12.99, 0.0, '2024-02-01', 8.99, 'PAID', 'North', 'Alice Johnson'),
('ORD-008', 'lisa.garcia@personal.com', 'Lisa Garcia', 'NOTEBOOK-001', 'Notebook Set', 'Office', 3, 8.99, 0.0, '2024-02-03', 4.99, 'PAID', 'South', 'Bob Wilson'),
('ORD-009', 'robert.martinez@freelance.com', 'Robert Martinez', 'PEN-001', 'Pen Set', 'Office', 10, 5.99, 5.0, '2024-02-05', 6.99, 'PAID', 'West', 'Carol Davis'),
-- Repeat purchases to test customer analytics
('ORD-010', 'john.doe@enterprise.com', 'John Doe', 'KEYBOARD-001', 'Mechanical Keyboard', 'Electronics', 1, 149.99, 0.0, '2024-02-10', 8.99, 'PAID', 'North', 'Alice Johnson'),
('ORD-011', 'sarah.johnson@corp.com', 'Sarah Johnson', 'LAMP-001', 'Desk Lamp', 'Furniture', 2, 89.99, 10.0, '2024-02-12', 12.99, 'PAID', 'South', 'Bob Wilson'),
-- Edge cases and data quality issues
('ORD-012', 'invalid-email', 'Test Customer', 'UNKNOWN-SKU', 'Unknown Product', 'Unknown', 1, 0.00, 0.0, '2024-02-15', 0.00, 'PENDING', 'Unknown', 'Unknown Rep'),
('ORD-013', 'jennifer.anderson@valid.com', 'Jennifer Anderson', 'SPEAKER-001', 'Bluetooth Speaker', 'Electronics', -1, 79.99, 0.0, '2024-02-18', 9.99, 'PAID', 'East', 'David Miller'),
('ORD-014', 'tom.johnson@test.com', 'Tom Johnson', 'CHAIR-BASIC-001', 'Basic Chair', 'Furniture', 1, 99.99, 100.0, '2024-02-20', 15.99, 'REFUNDED', 'North', 'Alice Johnson');

-- Insert customer segment definitions
INSERT INTO customer_segments (segment_name, min_orders, min_revenue, description) VALUES
('VIP', 5, 2000.00, 'High-value customers with multiple orders and high revenue'),
('Premium', 3, 1000.00, 'Regular customers with good purchasing power'),
('Standard', 2, 500.00, 'Regular customers with moderate purchasing'),
('New', 1, 0.00, 'New or low-activity customers');
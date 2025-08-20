-- Create raw_orders table (contains messy, inconsistent data)
CREATE TABLE raw_orders (
    order_id VARCHAR(50),
    customer_email VARCHAR(255),
    customer_name VARCHAR(255),
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    order_date VARCHAR(50),
    shipping_address TEXT,
    payment_status VARCHAR(50),
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create raw_inventory table (contains inventory movements)
CREATE TABLE raw_inventory (
    movement_id SERIAL PRIMARY KEY,
    product_sku VARCHAR(100),
    movement_type VARCHAR(50), -- 'IN', 'OUT', 'ADJUSTMENT', 'RETURN'
    quantity INTEGER,
    movement_date DATE,
    warehouse_location VARCHAR(100),
    reason_code VARCHAR(50),
    reference_id VARCHAR(100),
    unit_cost DECIMAL(8,2),
    source_system VARCHAR(50)
);

-- Create raw_customer_feedback table (contains customer reviews and ratings)
CREATE TABLE raw_customer_feedback (
    feedback_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    customer_email VARCHAR(255),
    product_sku VARCHAR(100),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_text TEXT,
    feedback_date DATE,
    sentiment_score DECIMAL(3,2),
    category VARCHAR(50),
    source_system VARCHAR(50)
);

-- Create product_catalog table (master product data)
CREATE TABLE product_catalog (
    product_sku VARCHAR(100) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    supplier_id VARCHAR(50),
    cost_price DECIMAL(8,2),
    retail_price DECIMAL(8,2),
    weight_kg DECIMAL(5,2),
    dimensions_cm VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert messy raw_orders data (simulating real-world data quality issues)
INSERT INTO raw_orders (order_id, customer_email, customer_name, product_sku, product_name, quantity, unit_price, order_date, shipping_address, payment_status, source_system) VALUES
('ORD-001', 'john.doe@email.com', 'John Doe', 'LAPTOP-001', 'Laptop Pro 15"', 1, 1299.99, '2024-01-15', '123 Main St, City, State 12345', 'PAID', 'ecommerce'),
('ORD-002', 'sarah.johnson@email.com', 'Sarah Johnson', 'MOUSE-001', 'Wireless Mouse', 2, 29.99, '2024-01-16', '456 Oak Ave, City, State 12345', 'PAID', 'ecommerce'),
('ORD-003', 'mike.brown@email.com', 'Mike Brown', 'CHAIR-001', 'Office Chair', 1, 199.99, '2024-01-17', '789 Pine Rd, City, State 12345', 'PENDING', 'ecommerce'),
('ORD-004', 'emily.davis@email.com', 'Emily Davis', 'LAMP-001', 'Desk Lamp', 1, 49.99, '2024-01-18', '321 Elm St, City, State 12345', 'PAID', 'ecommerce'),
('ORD-005', 'david.wilson@email.com', 'David Wilson', 'MUG-001', 'Coffee Mug', 3, 12.99, '2024-01-19', '654 Maple Dr, City, State 12345', 'PAID', 'ecommerce'),
('ORD-006', 'lisa.garcia@email.com', 'Lisa Garcia', 'NOTEBOOK-001', 'Notebook', 5, 8.99, '2024-01-20', '987 Cedar Ln, City, State 12345', 'PAID', 'ecommerce'),
('ORD-007', 'robert.martinez@email.com', 'Robert Martinez', 'PEN-001', 'Pen Set', 2, 15.99, '2024-01-21', '147 Birch Way, City, State 12345', 'PAID', 'ecommerce'),
('ORD-008', 'jennifer.anderson@email.com', 'Jennifer Anderson', 'SPEAKER-001', 'Bluetooth Speaker', 1, 79.99, '2024-01-22', '258 Spruce Ct, City, State 12345', 'PAID', 'ecommerce'),
-- Add some data quality issues
('ORD-009', 'invalid-email', 'John Smith', 'LAPTOP-001', 'Laptop Pro 15"', 1, 1299.99, '2024-01-23', '123 Main St, City, State 12345', 'PAID', 'ecommerce'),
('ORD-010', 'jane.smith@email.com', '', 'MOUSE-001', 'Wireless Mouse', -1, 29.99, '2024-01-24', '456 Oak Ave, City, State 12345', 'PAID', 'ecommerce'),
('ORD-011', 'bob.jones@email.com', 'Bob Jones', 'UNKNOWN-SKU', 'Unknown Product', 1, 0.00, '2024-01-25', '789 Pine Rd, City, State 12345', 'PAID', 'ecommerce'),
('ORD-012', 'alice.white@email.com', 'Alice White', 'CHAIR-001', 'Office Chair', 1, 199.99, 'invalid-date', '321 Elm St, City, State 12345', 'PAID', 'ecommerce');

-- Insert raw_inventory data
INSERT INTO raw_inventory (product_sku, movement_type, quantity, movement_date, warehouse_location, reason_code, reference_id, unit_cost, source_system) VALUES
('LAPTOP-001', 'IN', 50, '2024-01-01', 'WAREHOUSE-A', 'PURCHASE', 'PO-001', 800.00, 'inventory_system'),
('MOUSE-001', 'IN', 200, '2024-01-01', 'WAREHOUSE-A', 'PURCHASE', 'PO-002', 15.00, 'inventory_system'),
('CHAIR-001', 'IN', 30, '2024-01-01', 'WAREHOUSE-B', 'PURCHASE', 'PO-003', 120.00, 'inventory_system'),
('LAMP-001', 'IN', 100, '2024-01-01', 'WAREHOUSE-A', 'PURCHASE', 'PO-004', 25.00, 'inventory_system'),
('MUG-001', 'IN', 500, '2024-01-01', 'WAREHOUSE-A', 'PURCHASE', 'PO-005', 5.00, 'inventory_system'),
('NOTEBOOK-001', 'IN', 300, '2024-01-01', 'WAREHOUSE-A', 'PURCHASE', 'PO-006', 3.00, 'inventory_system'),
('PEN-001', 'IN', 150, '2024-01-01', 'WAREHOUSE-A', 'PURCHASE', 'PO-007', 8.00, 'inventory_system'),
('SPEAKER-001', 'IN', 75, '2024-01-01', 'WAREHOUSE-B', 'PURCHASE', 'PO-008', 45.00, 'inventory_system'),
-- Add some inventory movements
('LAPTOP-001', 'OUT', 1, '2024-01-15', 'WAREHOUSE-A', 'SALE', 'ORD-001', 800.00, 'inventory_system'),
('MOUSE-001', 'OUT', 2, '2024-01-16', 'WAREHOUSE-A', 'SALE', 'ORD-002', 15.00, 'inventory_system'),
('CHAIR-001', 'OUT', 1, '2024-01-17', 'WAREHOUSE-B', 'SALE', 'ORD-003', 120.00, 'inventory_system'),
('LAMP-001', 'OUT', 1, '2024-01-18', 'WAREHOUSE-A', 'SALE', 'ORD-004', 25.00, 'inventory_system'),
('MUG-001', 'OUT', 3, '2024-01-19', 'WAREHOUSE-A', 'SALE', 'ORD-005', 5.00, 'inventory_system'),
('NOTEBOOK-001', 'OUT', 5, '2024-01-20', 'WAREHOUSE-A', 'SALE', 'ORD-006', 3.00, 'inventory_system'),
('PEN-001', 'OUT', 2, '2024-01-21', 'WAREHOUSE-A', 'SALE', 'ORD-007', 8.00, 'inventory_system'),
('SPEAKER-001', 'OUT', 1, '2024-01-22', 'WAREHOUSE-B', 'SALE', 'ORD-008', 45.00, 'inventory_system'),
-- Add some adjustments and returns
('LAPTOP-001', 'ADJUSTMENT', -2, '2024-01-20', 'WAREHOUSE-A', 'DAMAGED', 'ADJ-001', 800.00, 'inventory_system'),
('MOUSE-001', 'RETURN', 1, '2024-01-25', 'WAREHOUSE-A', 'CUSTOMER_RETURN', 'ORD-002', 15.00, 'inventory_system');

-- Insert raw_customer_feedback data
INSERT INTO raw_customer_feedback (order_id, customer_email, product_sku, rating, review_text, feedback_date, sentiment_score, category, source_system) VALUES
('ORD-001', 'john.doe@email.com', 'LAPTOP-001', 5, 'Excellent laptop, fast performance!', '2024-01-20', 0.95, 'Electronics', 'feedback_system'),
('ORD-002', 'sarah.johnson@email.com', 'MOUSE-001', 4, 'Good wireless mouse, battery life could be better', '2024-01-21', 0.75, 'Electronics', 'feedback_system'),
('ORD-003', 'mike.brown@email.com', 'CHAIR-001', 3, 'Comfortable but expensive for what it is', '2024-01-22', 0.60, 'Furniture', 'feedback_system'),
('ORD-004', 'emily.davis@email.com', 'LAMP-001', 5, 'Perfect desk lamp, great lighting', '2024-01-23', 0.90, 'Furniture', 'feedback_system'),
('ORD-005', 'david.wilson@email.com', 'MUG-001', 4, 'Nice coffee mug, good quality', '2024-01-24', 0.80, 'Kitchen', 'feedback_system'),
('ORD-006', 'lisa.garcia@email.com', 'NOTEBOOK-001', 2, 'Pages are too thin, ink bleeds through', '2024-01-25', 0.30, 'Office', 'feedback_system'),
('ORD-007', 'robert.martinez@email.com', 'PEN-001', 5, 'Great pen set, smooth writing', '2024-01-26', 0.95, 'Office', 'feedback_system'),
('ORD-008', 'jennifer.anderson@email.com', 'SPEAKER-001', 4, 'Good sound quality, easy to connect', '2024-01-27', 0.85, 'Electronics', 'feedback_system'),
-- Add some invalid feedback
('ORD-009', 'invalid-email', 'LAPTOP-001', 6, 'Invalid rating', '2024-01-28', 0.50, 'Electronics', 'feedback_system'),
('ORD-010', 'jane.smith@email.com', 'MOUSE-001', 0, 'Invalid rating', '2024-01-29', 0.10, 'Electronics', 'feedback_system');

-- Insert product_catalog data
INSERT INTO product_catalog (product_sku, product_name, category, subcategory, brand, supplier_id, cost_price, retail_price, weight_kg, dimensions_cm, is_active, created_date) VALUES
('LAPTOP-001', 'Laptop Pro 15"', 'Electronics', 'Computers', 'TechCorp', 'SUP-001', 800.00, 1299.99, 2.5, '35x24x2', TRUE, '2023-01-01'),
('MOUSE-001', 'Wireless Mouse', 'Electronics', 'Accessories', 'TechCorp', 'SUP-001', 15.00, 29.99, 0.1, '12x6x3', TRUE, '2023-01-01'),
('CHAIR-001', 'Office Chair', 'Furniture', 'Seating', 'FurnitureCo', 'SUP-002', 120.00, 199.99, 15.0, '60x60x120', TRUE, '2023-01-01'),
('LAMP-001', 'Desk Lamp', 'Furniture', 'Lighting', 'LightingInc', 'SUP-003', 25.00, 49.99, 1.2, '30x15x45', TRUE, '2023-01-01'),
('MUG-001', 'Coffee Mug', 'Kitchen', 'Drinkware', 'KitchenSupplies', 'SUP-004', 5.00, 12.99, 0.3, '10x10x12', TRUE, '2023-01-01'),
('NOTEBOOK-001', 'Notebook', 'Office', 'Paper', 'PaperCo', 'SUP-005', 3.00, 8.99, 0.2, '21x15x1', TRUE, '2023-01-01'),
('PEN-001', 'Pen Set', 'Office', 'Writing', 'PaperCo', 'SUP-005', 8.00, 15.99, 0.1, '15x2x2', TRUE, '2023-01-01'),
('SPEAKER-001', 'Bluetooth Speaker', 'Electronics', 'Audio', 'AudioTech', 'SUP-006', 45.00, 79.99, 0.5, '15x8x8', TRUE, '2023-01-01');

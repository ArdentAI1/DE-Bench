-- Create tables for Amazon Seller Partner API data

-- Orders table
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    buyer_email VARCHAR(255),
    order_status VARCHAR(50),
    order_total DECIMAL(10,2)
);

-- Order items table
CREATE TABLE order_items (
    item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER,
    item_price DECIMAL(10,2)
);

-- Products table
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2)
);

-- Inventory table
CREATE TABLE inventory (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    quantity_available INTEGER,
    warehouse_location VARCHAR(100)
);

-- No initial data - all tables start empty and will be populated by the DAG

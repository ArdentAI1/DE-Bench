-- Source OLTP database schema for Data Vault ETL

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    registration_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2),
    supplier_id INTEGER,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    order_date DATE NOT NULL,
    status VARCHAR(50),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(50)
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2),
    discount DECIMAL(5,2) DEFAULT 0
);

-- Insert sample customers
INSERT INTO customers (name, email, phone, address, city, state, registration_date) VALUES
('John Smith', 'john.smith@example.com', '555-0101', '123 Main St', 'New York', 'NY', '2024-01-15'),
('Jane Doe', 'jane.doe@example.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '2024-01-20'),
('Bob Johnson', 'bob.johnson@example.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', '2024-02-01'),
('Alice Williams', 'alice.williams@example.com', '555-0104', '321 Elm St', 'Houston', 'TX', '2024-02-15'),
('Charlie Brown', 'charlie.brown@example.com', '555-0105', '654 Maple Ave', 'Phoenix', 'AZ', '2024-03-01');

-- Insert sample products
INSERT INTO products (name, category, price, supplier_id, description) VALUES
('Laptop Pro 15', 'Electronics', 1299.99, 101, 'High-performance laptop for professionals'),
('Wireless Mouse', 'Electronics', 29.99, 102, 'Ergonomic wireless mouse with precision tracking'),
('Office Chair', 'Furniture', 249.99, 103, 'Comfortable office chair with lumbar support'),
('Standing Desk', 'Furniture', 599.99, 103, 'Adjustable height standing desk'),
('USB-C Hub', 'Electronics', 49.99, 102, '7-in-1 USB-C hub with multiple ports');

-- Insert sample orders
INSERT INTO orders (customer_id, order_date, status, total_amount, payment_method) VALUES
(1, '2024-03-15', 'completed', 1299.99, 'credit_card'),
(2, '2024-03-16', 'completed', 279.98, 'paypal'),
(3, '2024-03-17', 'shipped', 849.98, 'credit_card'),
(1, '2024-03-18', 'completed', 49.99, 'credit_card'),
(4, '2024-03-19', 'processing', 599.99, 'paypal');

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES
(1, 1, 1, 1299.99, 0),
(2, 2, 1, 29.99, 0),
(2, 5, 1, 49.99, 5.00),
(3, 3, 1, 249.99, 0),
(3, 4, 1, 599.99, 0),
(4, 5, 1, 49.99, 0),
(5, 4, 1, 599.99, 0);

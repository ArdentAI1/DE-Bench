-- Create tables for logical replication test
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(100),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    inventory INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2),
    status VARCHAR(50)
);

-- Insert sample data
INSERT INTO users (email, name, region) VALUES
('user1@example.com', 'Alice Johnson', 'US'),
('user2@example.com', 'Bob Smith', 'US'),
('user3@example.com', 'Charlie Brown', 'EU'),
('user4@example.com', 'Diana Prince', 'EU'),
('user5@example.com', 'Eve Davis', 'ASIA');

INSERT INTO products (name, price, inventory) VALUES
('Laptop', 999.99, 50),
('Mouse', 29.99, 200),
('Keyboard', 79.99, 150),
('Monitor', 299.99, 75),
('Headphones', 149.99, 100);

INSERT INTO orders (user_id, order_date, total_amount, status) VALUES
(1, '2024-11-01', 999.99, 'completed'),
(2, '2024-11-02', 109.98, 'completed'),
(3, '2024-11-03', 299.99, 'pending'),
(4, '2024-11-04', 149.99, 'completed'),
(5, '2024-11-05', 79.99, 'shipped');

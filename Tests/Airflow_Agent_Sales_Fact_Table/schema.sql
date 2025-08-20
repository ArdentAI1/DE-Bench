-- Create transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(50),
    store_location VARCHAR(100)
);

-- Create items table
CREATE TABLE items (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit_price DECIMAL(8,2) NOT NULL,
    supplier VARCHAR(100)
);

-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT
);

-- Insert sample transaction data
INSERT INTO transactions (transaction_date, total_amount, payment_method, store_location) VALUES
('2024-01-15', 299.99, 'Credit Card', 'Downtown Store'),
('2024-01-16', 149.50, 'Cash', 'Mall Location'),
('2024-01-17', 89.99, 'Debit Card', 'Downtown Store'),
('2024-01-18', 199.99, 'Credit Card', 'Mall Location'),
('2024-01-19', 75.25, 'Cash', 'Downtown Store');

-- Insert sample item data
INSERT INTO items (item_name, category, unit_price, supplier) VALUES
('Laptop Pro', 'Electronics', 1299.99, 'TechCorp'),
('Wireless Mouse', 'Electronics', 29.99, 'TechCorp'),
('Office Chair', 'Furniture', 199.99, 'FurnitureCo'),
('Desk Lamp', 'Furniture', 49.99, 'LightingInc'),
('Coffee Mug', 'Kitchen', 12.99, 'KitchenSupplies'),
('Notebook', 'Office', 8.99, 'PaperCo'),
('Pen Set', 'Office', 15.99, 'PaperCo'),
('Bluetooth Speaker', 'Electronics', 79.99, 'AudioTech');

-- Insert sample customer data
INSERT INTO customers (first_name, last_name, email, phone, address) VALUES
('John', 'Smith', 'john.smith@email.com', '555-0101', '123 Main St, City, State'),
('Sarah', 'Johnson', 'sarah.johnson@email.com', '555-0102', '456 Oak Ave, City, State'),
('Michael', 'Brown', 'michael.brown@email.com', '555-0103', '789 Pine Rd, City, State'),
('Emily', 'Davis', 'emily.davis@email.com', '555-0104', '321 Elm St, City, State'),
('David', 'Wilson', 'david.wilson@email.com', '555-0105', '654 Maple Dr, City, State'),
('Lisa', 'Garcia', 'lisa.garcia@email.com', '555-0106', '987 Cedar Ln, City, State'),
('Robert', 'Martinez', 'robert.martinez@email.com', '555-0107', '147 Birch Way, City, State'),
('Jennifer', 'Anderson', 'jennifer.anderson@email.com', '555-0108', '258 Spruce Ct, City, State');

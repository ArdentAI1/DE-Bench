-- Create transactions table for sales data
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    sale_amount DECIMAL(10,2) NOT NULL,
    cost_amount DECIMAL(10,2) NOT NULL,
    transaction_date DATE NOT NULL
);

-- Insert test transaction data
INSERT INTO transactions (user_id, product_id, sale_amount, cost_amount, transaction_date) VALUES
(1, 101, 100.00, 60.00, '2024-01-01'),
(1, 102, 150.00, 90.00, '2024-01-01'),
(2, 101, 200.00, 120.00, '2024-01-01');

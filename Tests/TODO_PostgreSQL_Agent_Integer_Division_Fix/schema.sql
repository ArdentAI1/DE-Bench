-- Create purchases_bad table with integer division issue
-- This table has INT columns that cause division truncation (e.g., 5/10 = 0 instead of 0.5)
CREATE TABLE purchases_bad (
    user_id INT PRIMARY KEY,
    total_items INT NOT NULL,
    total_orders INT NOT NULL
);

-- Insert test data that demonstrates the integer division problem
INSERT INTO purchases_bad (user_id, total_items, total_orders) VALUES
(1, 5, 10),   -- Should be 0.5 items per order, but INT/INT = 0
(2, 3, 7),    -- Should be ~0.4286 items per order, but INT/INT = 0  
(4, 3, 4);    -- Should be 0.75 items per order, but INT/INT = 0

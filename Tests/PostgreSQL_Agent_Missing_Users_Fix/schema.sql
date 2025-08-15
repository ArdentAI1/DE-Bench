-- Create users table
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    name TEXT NOT NULL
);

-- Create subscriptions_bad table (intentionally bad design - no FK constraints)
CREATE TABLE subscriptions_bad (
    user_id INT,  -- No PK, no FK constraints
    plan TEXT     -- Free text, not normalized
);

-- Insert test data
INSERT INTO users (user_id, name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Carol');  -- Carol has no subscription!

INSERT INTO subscriptions_bad (user_id, plan) VALUES
(1, 'Pro'),
(2, 'Basic');
-- Carol (user_id 3) has no subscription row at all

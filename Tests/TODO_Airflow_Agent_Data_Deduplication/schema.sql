-- Create users_source_1 table
CREATE TABLE users_source_1 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    company VARCHAR(100),
    source VARCHAR(50) DEFAULT 'source_1'
);

-- Create users_source_2 table
CREATE TABLE users_source_2 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    department VARCHAR(100),
    source VARCHAR(50) DEFAULT 'source_2'
);

-- Create users_source_3 table
CREATE TABLE users_source_3 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(100),
    source VARCHAR(50) DEFAULT 'source_3'
);

-- Insert data into users_source_1
INSERT INTO users_source_1 (email, first_name, last_name, company) VALUES
('john.doe@example.com', 'John', 'Doe', 'Tech Corp'),
('jane.smith@example.com', 'Jane', 'Smith', 'Data Inc'),
('bob.wilson@example.com', 'Bob', 'Wilson', 'Startup LLC'),
('alice.johnson@example.com', 'Alice', 'Johnson', 'Big Corp');

-- Insert data into users_source_2
INSERT INTO users_source_2 (email, first_name, last_name, department) VALUES
('john.doe@example.com', 'John', 'Doe', 'Engineering'),
('sarah.brown@example.com', 'Sarah', 'Brown', 'Marketing'),
('mike.davis@example.com', 'Mike', 'Davis', 'Sales'),
('lisa.garcia@example.com', 'Lisa', 'Garcia', 'HR');

-- Insert data into users_source_3
INSERT INTO users_source_3 (email, first_name, last_name, role) VALUES
('jane.smith@example.com', 'Jane', 'Smith', 'Manager'),
('bob.wilson@example.com', 'Bob', 'Wilson', 'Developer'),
('emma.taylor@example.com', 'Emma', 'Taylor', 'Designer'),
('david.lee@example.com', 'David', 'Lee', 'Analyst');

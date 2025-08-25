-- Create users_source_1 table
CREATE TABLE users_source_1 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    company VARCHAR(100),
    registration_date DATE DEFAULT CURRENT_DATE,
    source VARCHAR(50) DEFAULT 'source_1'
);

-- Create users_source_2 table
CREATE TABLE users_source_2 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    department VARCHAR(100),
    join_date DATE DEFAULT CURRENT_DATE,
    source VARCHAR(50) DEFAULT 'source_2'
);

-- Create users_source_3 table
CREATE TABLE users_source_3 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(100),
    start_date DATE DEFAULT CURRENT_DATE,
    source VARCHAR(50) DEFAULT 'source_3'
);

-- Insert data into users_source_1 (includes duplicates that exist in other sources)
INSERT INTO users_source_1 (email, first_name, last_name, company, registration_date) VALUES
('john.doe@example.com', 'John', 'Doe', 'Tech Corp', '2024-01-01'),
('jane.smith@example.com', 'Jane', 'Smith', 'Data Inc', '2024-01-02'),
('bob.wilson@example.com', 'Bob', 'Wilson', 'Startup LLC', '2024-01-03'),
('alice.johnson@example.com', 'Alice', 'Johnson', 'Big Corp', '2024-01-04'),
('charlie.brown@example.com', 'Charlie', 'Brown', 'Small Biz', '2024-01-05');

-- Insert data into users_source_2 (includes some overlapping emails)
INSERT INTO users_source_2 (email, first_name, last_name, department, join_date) VALUES
('john.doe@example.com', 'John', 'Doe', 'Engineering', '2024-01-10'),
('sarah.brown@example.com', 'Sarah', 'Brown', 'Marketing', '2024-01-11'),
('mike.davis@example.com', 'Mike', 'Davis', 'Sales', '2024-01-12'),
('lisa.garcia@example.com', 'Lisa', 'Garcia', 'HR', '2024-01-13'),
('jane.smith@example.com', 'Jane', 'Smith', 'Product', '2024-01-14');

-- Insert data into users_source_3 (includes some overlapping emails)
INSERT INTO users_source_3 (email, first_name, last_name, role, start_date) VALUES
('bob.wilson@example.com', 'Bob', 'Wilson', 'Senior Developer', '2024-01-20'),
('emma.taylor@example.com', 'Emma', 'Taylor', 'UX Designer', '2024-01-21'),
('david.lee@example.com', 'David', 'Lee', 'Data Analyst', '2024-01-22'),
('jane.smith@example.com', 'Jane', 'Smith', 'Product Manager', '2024-01-23'),
('tom.johnson@example.com', 'Tom', 'Johnson', 'DevOps Engineer', '2024-01-24');
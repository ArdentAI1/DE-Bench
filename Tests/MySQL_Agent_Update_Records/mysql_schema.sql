-- MySQL schema for update records test
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial test data
INSERT INTO users (name, email, age) VALUES
('John Doe', 'john@example.com', 32),
('Jane Smith', 'jane@example.com', 25),
('Bob Johnson', 'bob@example.com', 38),
('Carol White', 'carol@example.com', 29);

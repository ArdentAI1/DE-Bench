User_Input = """
Objective: Normalize a given denormalized table schema into Third Normal Form (3NF) to improve database efficiency and reduce redundancy.

Parameters to be Provided:
PostgreSQL Instance:

Connection details (host, port, database name, user, password)
Denormalized Table Schema:

Table Name: orders
Columns and Sample Data:
sql
Copy code
CREATE TABLE orders (
  order_id INT,
  customer_name VARCHAR(100),
  customer_address VARCHAR(255),
  product_id INT,
  product_name VARCHAR(100),
  product_price DECIMAL(10, 2),
  order_date DATE,
  quantity INT,
  total_price DECIMAL(10, 2)
);
Normalization Rules: Convert the denormalized table into Third Normal Form (3NF).

Tasks:
Analyze the provided denormalized table schema.
Identify functional dependencies and anomalies.
Decompose the table into multiple tables to achieve 3NF.
Provide the intermediate steps and resulting table structures for 1NF, 2NF, and 3NF.
Migrate the existing data from the orders table in the PostgreSQL instance into the new normalized tables.

Steps:
Analyze Schema: Examine the orders table to identify dependencies and redundancies.
Identify Functional Dependencies: Determine primary keys and how other attributes depend on them.
Decompose Tables: Split the orders table into smaller tables that meet 3NF requirements, ensuring each table has a primary key and that non-key attributes depend only on the primary key.
Update Database: Create the new tables in the PostgreSQL instance and migrate data accordingly.

Output:
Files to be Created:

normalize_to_3nf.sql: SQL script to create the normalized tables and migrate data.
Ensure all files are placed in the Test_Environment directory.
Include the table structures and SQL scripts for:

The initial table in First Normal Form (1NF).
The intermediate tables in Second Normal Form (2NF).
The final tables in Third Normal Form (3NF).
Please fill in solution code with comments and create files as you see fit. Output files in Test_Environment directory.
"""

Model_Configs = ""
Output_File_Location = """"""
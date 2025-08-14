-- Create tesla_stock table for financial data
CREATE TABLE tesla_stock (
    date DATE PRIMARY KEY,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT
);

-- No initial data - table starts empty and will be populated by the DAG

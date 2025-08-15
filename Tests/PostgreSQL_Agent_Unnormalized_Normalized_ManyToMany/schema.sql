-- Create books_bad table (unnormalized - 1NF violation with multi-valued authors field)
CREATE TABLE books_bad (
    book_id INT PRIMARY KEY,
    title TEXT NOT NULL,
    authors TEXT NOT NULL  -- 1NF violation: multiple authors in single field
);

-- Insert test data demonstrating 1NF violation
INSERT INTO books_bad (book_id, title, authors) VALUES
(1, 'Design Patterns', 'Gamma,Others'),      -- Multiple authors comma-separated
(2, 'Clean Code', 'Robert Martin');          -- Single author

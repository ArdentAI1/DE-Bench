-- Create products table with rich text content for full-text search
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    price NUMERIC(10,2),
    brand VARCHAR(100),
    tags TEXT[],
    specifications JSONB,
    popularity_score INTEGER DEFAULT 0,
    sales_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on category for filtering
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);

-- Insert sample products with diverse content
INSERT INTO products (name, description, category, price, brand, tags, specifications, popularity_score, sales_count) VALUES
-- Electronics
('Apple MacBook Pro 16"', 'Powerful laptop with M2 Pro chip, 16GB RAM, 512GB SSD. Perfect for developers and creative professionals.', 'Electronics', 2499.99, 'Apple', ARRAY['laptop', 'computer', 'macbook', 'professional'], '{"cpu": "M2 Pro", "ram": "16GB", "storage": "512GB SSD", "screen": "16 inch", "color": "Space Gray"}', 95, 450),
('Sony WH-1000XM5 Headphones', 'Premium noise-cancelling wireless headphones with exceptional sound quality and 30-hour battery life.', 'Electronics', 399.99, 'Sony', ARRAY['headphones', 'wireless', 'noise-cancelling', 'audio'], '{"type": "Over-ear", "wireless": true, "battery": "30 hours", "noise_cancelling": true}', 88, 680),
('Samsung Galaxy S23 Ultra', 'Flagship smartphone with 200MP camera, S Pen, and stunning 6.8-inch display.', 'Electronics', 1199.99, 'Samsung', ARRAY['smartphone', 'phone', 'android', 'camera'], '{"camera": "200MP", "screen": "6.8 inch", "storage": "256GB", "5g": true}', 92, 1200),
('Dell XPS 13 Laptop', 'Ultra-portable laptop with Intel i7 processor, 16GB RAM, and beautiful InfinityEdge display.', 'Electronics', 1299.99, 'Dell', ARRAY['laptop', 'ultrabook', 'portable', 'windows'], '{"cpu": "Intel i7", "ram": "16GB", "storage": "512GB SSD", "screen": "13.4 inch"}', 82, 320),
('iPad Pro 12.9"', 'Professional tablet with M2 chip, stunning Liquid Retina XDR display, and Apple Pencil support.', 'Electronics', 1099.99, 'Apple', ARRAY['tablet', 'ipad', 'professional', 'drawing'], '{"cpu": "M2", "screen": "12.9 inch", "storage": "128GB", "pencil_support": true}', 87, 540),

-- Clothing
('Nike Air Max 270 Running Shoes', 'Comfortable running shoes with Air cushioning technology. Available in multiple colors.', 'Clothing', 149.99, 'Nike', ARRAY['shoes', 'running', 'sports', 'athletic'], '{"size_range": "6-13", "material": "mesh and synthetic", "colors": ["black", "white", "red"]}', 79, 890),
('Levi''s 501 Original Jeans', 'Classic straight-fit jeans made from premium denim. Timeless style for everyday wear.', 'Clothing', 69.99, 'Levis', ARRAY['jeans', 'denim', 'pants', 'casual'], '{"fit": "straight", "material": "100% cotton denim", "sizes": ["28-38"]}', 85, 1450),
('Patagonia Down Jacket', 'Warm and lightweight down jacket perfect for cold weather. Sustainable and eco-friendly.', 'Clothing', 279.99, 'Patagonia', ARRAY['jacket', 'winter', 'outdoor', 'warm'], '{"insulation": "down", "water_resistant": true, "temperature": "-10C to 5C"}', 91, 420),
('Adidas Ultraboost Running Shoes', 'High-performance running shoes with responsive Boost cushioning and Primeknit upper.', 'Clothing', 189.99, 'Adidas', ARRAY['shoes', 'running', 'sports', 'performance'], '{"technology": "Boost", "material": "Primeknit", "weight": "310g"}', 88, 760),
('Red Leather Jacket', 'Stylish genuine leather jacket in vibrant red color. Perfect for motorcycle riders.', 'Clothing', 399.99, 'Unknown', ARRAY['jacket', 'leather', 'motorcycle', 'red'], '{"material": "genuine leather", "color": "red", "style": "biker"}', 76, 180),

-- Home & Kitchen
('Dyson V15 Vacuum Cleaner', 'Cordless vacuum with laser dust detection and powerful suction. Perfect for all floor types.', 'Home & Kitchen', 649.99, 'Dyson', ARRAY['vacuum', 'cordless', 'cleaning', 'home'], '{"type": "cordless", "battery": "60 minutes", "filtration": "HEPA"}', 90, 340),
('Ninja Air Fryer', 'Large capacity air fryer with 6 cooking functions. Healthy cooking with little to no oil.', 'Home & Kitchen', 129.99, 'Ninja', ARRAY['air fryer', 'kitchen', 'cooking', 'appliance'], '{"capacity": "5.5 quarts", "functions": 6, "temperature": "up to 400F"}', 86, 920),
('KitchenAid Stand Mixer', 'Professional-grade stand mixer with 10 speeds and multiple attachments. Baker''s choice.', 'Home & Kitchen', 449.99, 'KitchenAid', ARRAY['mixer', 'kitchen', 'baking', 'appliance'], '{"capacity": "5 quart", "speeds": 10, "power": "325 watts"}', 93, 510),
('Instant Pot Duo', 'Multi-functional pressure cooker that also works as slow cooker, rice cooker, and more.', 'Home & Kitchen', 99.99, 'Instant Pot', ARRAY['pressure cooker', 'kitchen', 'cooking', 'multi-function'], '{"capacity": "6 quart", "functions": 7, "pressure_cooking": true}', 89, 1100),

-- Sports & Outdoors
('Yeti Rambler Tumbler', 'Insulated stainless steel tumbler keeps drinks cold for 24 hours or hot for 12 hours.', 'Sports & Outdoors', 34.99, 'Yeti', ARRAY['tumbler', 'insulated', 'drinkware', 'outdoor'], '{"capacity": "30 oz", "material": "stainless steel", "insulated": true}', 84, 1580),
('REI Co-op Tent', 'Spacious 4-person camping tent with easy setup and excellent weather protection.', 'Sports & Outdoors', 299.99, 'REI', ARRAY['tent', 'camping', 'outdoor', 'shelter'], '{"capacity": "4 person", "seasons": "3-season", "weight": "8 lbs"}', 87, 280),
('Hydroflask Water Bottle', 'Durable vacuum-insulated water bottle that keeps drinks cold for 24 hours.', 'Sports & Outdoors', 44.99, 'Hydroflask', ARRAY['water bottle', 'insulated', 'hydration', 'outdoor'], '{"capacity": "32 oz", "insulated": true, "bpa_free": true}', 91, 1340),

-- Books
('The Great Gatsby', 'Classic American novel by F. Scott Fitzgerald about the Jazz Age and the American Dream.', 'Books', 14.99, 'Scribner', ARRAY['book', 'fiction', 'classic', 'literature'], '{"author": "F. Scott Fitzgerald", "pages": 180, "isbn": "9780743273565"}', 78, 2400),
('Atomic Habits', 'Practical guide to building good habits and breaking bad ones by James Clear.', 'Books', 16.99, 'Avery', ARRAY['book', 'self-help', 'habits', 'productivity'], '{"author": "James Clear", "pages": 320, "isbn": "9780735211292"}', 94, 3200),
('The Lean Startup', 'Revolutionary approach to building startups and launching new products by Eric Ries.', 'Books', 17.99, 'Crown Business', ARRAY['book', 'business', 'startup', 'entrepreneurship'], '{"author": "Eric Ries", "pages": 336, "isbn": "9780307887894"}', 89, 1800);

-- Insert more products to reach a good dataset size
INSERT INTO products (name, description, category, price, brand, tags, specifications, popularity_score, sales_count)
SELECT 
    'Product ' || generate_series AS name,
    'Description for product ' || generate_series || '. This product offers great value and quality.' AS description,
    CASE (generate_series % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Home & Kitchen'
        WHEN 3 THEN 'Sports & Outdoors'
        ELSE 'Books'
    END AS category,
    (random() * 1000 + 10)::NUMERIC(10,2) AS price,
    'Brand ' || ((generate_series % 10) + 1) AS brand,
    ARRAY['tag' || (generate_series % 5), 'quality', 'popular'] AS tags,
    jsonb_build_object(
        'weight', ((random() * 10)::NUMERIC(5,2))::TEXT || ' lbs',
        'dimensions', ((random() * 20)::NUMERIC(5,2))::TEXT || ' inches',
        'warranty', (CASE WHEN random() > 0.5 THEN '1 year' ELSE '2 years' END)
    ) AS specifications,
    (random() * 100)::INTEGER AS popularity_score,
    (random() * 2000)::INTEGER AS sales_count
FROM generate_series(1, 100);

-- Create search analytics table
CREATE TABLE search_queries (
    id SERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    result_count INTEGER,
    execution_time_ms NUMERIC(10,2),
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add some sample search queries for testing
INSERT INTO search_queries (query_text, result_count, execution_time_ms) VALUES
('laptop', 0, 0),
('nike shoes', 0, 0),
('wireless headphones', 0, 0);

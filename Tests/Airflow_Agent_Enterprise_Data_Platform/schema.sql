-- ============================================================================
-- ENTERPRISE DATA PLATFORM SCHEMA
-- Simulates a complex enterprise environment with multiple data sources
-- ============================================================================

-- ============================================================================
-- CUSTOMER DATA (Multiple Sources)
-- ============================================================================

-- CRM System Data (Salesforce-like)
CREATE TABLE crm_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    account_name VARCHAR(255),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    industry VARCHAR(100),
    company_size VARCHAR(50),
    annual_revenue DECIMAL(15,2),
    lead_source VARCHAR(100),
    status VARCHAR(50),
    created_date TIMESTAMP,
    last_modified TIMESTAMP,
    owner_id VARCHAR(50),
    region VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'crm_system'
);

-- E-commerce Platform Data (Shopify-like)
CREATE TABLE ecommerce_customers (
    shopify_customer_id BIGINT PRIMARY KEY,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    total_spent DECIMAL(10,2),
    orders_count INTEGER,
    tags TEXT,
    note TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    accepts_marketing BOOLEAN,
    default_address JSONB,
    source_system VARCHAR(50) DEFAULT 'shopify'
);

-- Marketing Automation Data (HubSpot-like)
CREATE TABLE marketing_contacts (
    contact_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    company VARCHAR(255),
    lifecycle_stage VARCHAR(50),
    lead_status VARCHAR(50),
    lead_score INTEGER,
    subscription_status VARCHAR(50),
    last_activity_date TIMESTAMP,
    created_date TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'hubspot'
);

-- ============================================================================
-- TRANSACTION DATA (Multiple Systems)
-- ============================================================================

-- ERP System Transactions (NetSuite-like)
CREATE TABLE erp_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    transaction_date DATE,
    transaction_type VARCHAR(50), -- 'SALE', 'REFUND', 'ADJUSTMENT'
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    currency VARCHAR(3),
    tax_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    warehouse_location VARCHAR(100),
    sales_rep_id VARCHAR(50),
    source_system VARCHAR(50) DEFAULT 'netsuite'
);

-- E-commerce Transactions (Shopify-like)
CREATE TABLE ecommerce_orders (
    order_id BIGINT PRIMARY KEY,
    shopify_customer_id BIGINT,
    order_number VARCHAR(50),
    order_date TIMESTAMP,
    total_price DECIMAL(10,2),
    subtotal_price DECIMAL(10,2),
    total_tax DECIMAL(10,2),
    total_discount DECIMAL(10,2),
    currency VARCHAR(3),
    financial_status VARCHAR(50),
    fulfillment_status VARCHAR(50),
    shipping_address JSONB,
    billing_address JSONB,
    tags TEXT,
    note TEXT,
    source_system VARCHAR(50) DEFAULT 'shopify'
);

-- E-commerce Order Items
CREATE TABLE ecommerce_order_items (
    item_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    product_id BIGINT,
    variant_id BIGINT,
    product_sku VARCHAR(100),
    product_title VARCHAR(255),
    variant_title VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    source_system VARCHAR(50) DEFAULT 'shopify'
);

-- ============================================================================
-- PRODUCT DATA (Multiple Catalogs)
-- ============================================================================

-- ERP Product Catalog (NetSuite-like)
CREATE TABLE erp_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    supplier_id VARCHAR(50),
    cost_price DECIMAL(8,2),
    retail_price DECIMAL(8,2),
    weight_kg DECIMAL(5,2),
    dimensions_cm VARCHAR(50),
    is_active BOOLEAN,
    created_date DATE,
    last_modified TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'netsuite'
);

-- E-commerce Product Catalog (Shopify-like)
CREATE TABLE ecommerce_products (
    product_id BIGINT PRIMARY KEY,
    product_sku VARCHAR(100),
    title VARCHAR(255),
    body_html TEXT,
    vendor VARCHAR(100),
    product_type VARCHAR(100),
    tags TEXT,
    status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    published_at TIMESTAMP,
    template_suffix VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'shopify'
);

-- ============================================================================
-- INVENTORY DATA (Multiple Warehouses)
-- ============================================================================

-- ERP Inventory (NetSuite-like)
CREATE TABLE erp_inventory (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50),
    warehouse_id VARCHAR(50),
    location_name VARCHAR(100),
    quantity_on_hand INTEGER,
    quantity_committed INTEGER,
    quantity_available INTEGER,
    reorder_point INTEGER,
    preferred_stock_level INTEGER,
    last_count_date DATE,
    unit_cost DECIMAL(8,2),
    source_system VARCHAR(50) DEFAULT 'netsuite'
);

-- 3PL Warehouse Inventory (External System)
CREATE TABLE third_party_inventory (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_sku VARCHAR(100),
    warehouse_code VARCHAR(50),
    warehouse_name VARCHAR(100),
    quantity_available INTEGER,
    quantity_reserved INTEGER,
    quantity_damaged INTEGER,
    last_updated TIMESTAMP,
    api_version VARCHAR(20),
    source_system VARCHAR(50) DEFAULT '3pl_system'
);

-- ============================================================================
-- MARKETING DATA (Multiple Channels)
-- ============================================================================

-- Email Marketing Campaigns
CREATE TABLE email_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255),
    campaign_type VARCHAR(50),
    subject_line VARCHAR(255),
    sender_email VARCHAR(255),
    send_date TIMESTAMP,
    total_sent INTEGER,
    total_opens INTEGER,
    total_clicks INTEGER,
    total_bounces INTEGER,
    total_unsubscribes INTEGER,
    open_rate DECIMAL(5,2),
    click_rate DECIMAL(5,2),
    bounce_rate DECIMAL(5,2),
    source_system VARCHAR(50) DEFAULT 'email_platform'
);

-- Email Campaign Recipients
CREATE TABLE email_campaign_recipients (
    recipient_id VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(50),
    contact_email VARCHAR(255),
    contact_id VARCHAR(50),
    sent_date TIMESTAMP,
    opened_date TIMESTAMP,
    clicked_date TIMESTAMP,
    bounced BOOLEAN,
    unsubscribed BOOLEAN,
    source_system VARCHAR(50) DEFAULT 'email_platform'
);

-- Social Media Campaigns
CREATE TABLE social_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    platform VARCHAR(50), -- 'facebook', 'instagram', 'linkedin', 'twitter'
    campaign_name VARCHAR(255),
    campaign_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(10,2),
    spend DECIMAL(10,2),
    impressions INTEGER,
    clicks INTEGER,
    conversions INTEGER,
    ctr DECIMAL(5,2),
    cpc DECIMAL(8,2),
    cpm DECIMAL(8,2),
    source_system VARCHAR(50) DEFAULT 'social_platform'
);

-- ============================================================================
-- CUSTOMER FEEDBACK DATA (Multiple Sources)
-- ============================================================================

-- Product Reviews (E-commerce)
CREATE TABLE product_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    product_id BIGINT,
    customer_email VARCHAR(255),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_title VARCHAR(255),
    review_text TEXT,
    review_date TIMESTAMP,
    verified_purchase BOOLEAN,
    helpful_votes INTEGER,
    source_system VARCHAR(50) DEFAULT 'ecommerce'
);

-- Customer Support Tickets
CREATE TABLE support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    customer_email VARCHAR(255),
    customer_id VARCHAR(50),
    ticket_number VARCHAR(50),
    subject VARCHAR(255),
    description TEXT,
    priority VARCHAR(20),
    status VARCHAR(50),
    category VARCHAR(100),
    assigned_to VARCHAR(100),
    created_date TIMESTAMP,
    resolved_date TIMESTAMP,
    resolution_time_hours INTEGER,
    customer_satisfaction_rating INTEGER,
    source_system VARCHAR(50) DEFAULT 'support_system'
);

-- ============================================================================
-- FINANCIAL DATA (Multiple Systems)
-- ============================================================================

-- Accounting Transactions (QuickBooks-like)
CREATE TABLE accounting_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_date DATE,
    transaction_type VARCHAR(50),
    account_name VARCHAR(100),
    account_type VARCHAR(50),
    description TEXT,
    amount DECIMAL(12,2),
    debit_amount DECIMAL(12,2),
    credit_amount DECIMAL(12,2),
    reference_number VARCHAR(50),
    customer_id VARCHAR(50),
    vendor_id VARCHAR(50),
    class VARCHAR(100),
    location VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'quickbooks'
);

-- ============================================================================
-- OPERATIONAL DATA (Multiple Sources)
-- ============================================================================

-- Shipping and Fulfillment
CREATE TABLE shipping_orders (
    shipping_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    tracking_number VARCHAR(100),
    carrier VARCHAR(50),
    service_level VARCHAR(50),
    shipped_date TIMESTAMP,
    delivered_date TIMESTAMP,
    shipping_cost DECIMAL(8,2),
    package_weight DECIMAL(5,2),
    package_dimensions VARCHAR(50),
    delivery_status VARCHAR(50),
    source_system VARCHAR(50) DEFAULT 'shipping_system'
);

-- ============================================================================
-- INSERT SAMPLE DATA
-- ============================================================================

-- CRM Customers
INSERT INTO crm_customers VALUES
('CUST-001', 'Acme Corporation', 'john.doe@acme.com', '+1-555-0101', 'Technology', 'Enterprise', 5000000.00, 'Website', 'Active', '2023-01-15 10:00:00', '2024-01-15 14:30:00', 'SALES-001', 'North America', 'crm_system'),
('CUST-002', 'TechStart Inc', 'sarah.smith@techstart.com', '+1-555-0102', 'Technology', 'SMB', 500000.00, 'Trade Show', 'Active', '2023-02-20 09:15:00', '2024-01-10 16:45:00', 'SALES-002', 'North America', 'crm_system'),
('CUST-003', 'Global Retail Co', 'mike.johnson@globalretail.com', '+1-555-0103', 'Retail', 'Enterprise', 25000000.00, 'Referral', 'Active', '2023-03-10 11:30:00', '2024-01-12 13:20:00', 'SALES-003', 'Europe', 'crm_system'),
('CUST-004', 'StartupXYZ', 'emily.davis@startupxyz.com', '+1-555-0104', 'Technology', 'Startup', 100000.00, 'Social Media', 'Prospect', '2023-04-05 15:45:00', '2024-01-08 10:15:00', 'SALES-001', 'North America', 'crm_system'),
('CUST-005', 'Manufacturing Corp', 'david.wilson@mfgcorp.com', '+1-555-0105', 'Manufacturing', 'Enterprise', 15000000.00, 'Cold Call', 'Active', '2023-05-12 08:30:00', '2024-01-14 17:00:00', 'SALES-002', 'Asia Pacific', 'crm_system');

-- E-commerce Customers
INSERT INTO ecommerce_customers VALUES
(1001, 'john.doe@acme.com', 'John', 'Doe', '+1-555-0101', 2500.00, 5, 'vip,enterprise', 'Enterprise customer', '2023-01-15 10:00:00', '2024-01-15 14:30:00', TRUE, '{"address1":"123 Main St","city":"New York","state":"NY","zip":"10001"}', 'shopify'),
(1002, 'sarah.smith@techstart.com', 'Sarah', 'Smith', '+1-555-0102', 1200.00, 3, 'smb,tech', 'SMB customer', '2023-02-20 09:15:00', '2024-01-10 16:45:00', TRUE, '{"address1":"456 Oak Ave","city":"San Francisco","state":"CA","zip":"94102"}', 'shopify'),
(1003, 'mike.johnson@globalretail.com', 'Mike', 'Johnson', '+1-555-0103', 5000.00, 8, 'enterprise,retail', 'Enterprise retail customer', '2023-03-10 11:30:00', '2024-01-12 13:20:00', FALSE, '{"address1":"789 Pine Rd","city":"London","state":"","zip":"SW1A 1AA"}', 'shopify'),
(1004, 'emily.davis@startupxyz.com', 'Emily', 'Davis', '+1-555-0104', 300.00, 1, 'startup', 'Startup customer', '2023-04-05 15:45:00', '2024-01-08 10:15:00', TRUE, '{"address1":"321 Elm St","city":"Austin","state":"TX","zip":"73301"}', 'shopify'),
(1005, 'david.wilson@mfgcorp.com', 'David', 'Wilson', '+1-555-0105', 1800.00, 4, 'enterprise,manufacturing', 'Manufacturing customer', '2023-05-12 08:30:00', '2024-01-14 17:00:00', TRUE, '{"address1":"654 Maple Dr","city":"Tokyo","state":"","zip":"100-0001"}', 'shopify');

-- Marketing Contacts
INSERT INTO marketing_contacts VALUES
('CONT-001', 'john.doe@acme.com', 'John', 'Doe', 'Acme Corporation', 'Customer', 'Active', 85, 'Subscribed', '2024-01-15 14:30:00', '2023-01-15 10:00:00', 'hubspot'),
('CONT-002', 'sarah.smith@techstart.com', 'Sarah', 'Smith', 'TechStart Inc', 'Customer', 'Active', 72, 'Subscribed', '2024-01-10 16:45:00', '2023-02-20 09:15:00', 'hubspot'),
('CONT-003', 'mike.johnson@globalretail.com', 'Mike', 'Johnson', 'Global Retail Co', 'Customer', 'Active', 95, 'Subscribed', '2024-01-12 13:20:00', '2023-03-10 11:30:00', 'hubspot'),
('CONT-004', 'emily.davis@startupxyz.com', 'Emily', 'Davis', 'StartupXYZ', 'Lead', 'New', 45, 'Subscribed', '2024-01-08 10:15:00', '2023-04-05 15:45:00', 'hubspot'),
('CONT-005', 'david.wilson@mfgcorp.com', 'David', 'Wilson', 'Manufacturing Corp', 'Customer', 'Active', 78, 'Subscribed', '2024-01-14 17:00:00', '2023-05-12 08:30:00', 'hubspot');

-- ERP Products
INSERT INTO erp_products VALUES
('PROD-001', 'LAPTOP-PRO-15', 'Laptop Pro 15"', 'Electronics', 'Computers', 'TechCorp', 'SUP-001', 800.00, 1299.99, 2.5, '35x24x2', TRUE, '2023-01-01', '2024-01-15 10:00:00', 'netsuite'),
('PROD-002', 'MOUSE-WIRELESS', 'Wireless Mouse', 'Electronics', 'Accessories', 'TechCorp', 'SUP-001', 15.00, 29.99, 0.1, '12x6x3', TRUE, '2023-01-01', '2024-01-15 10:00:00', 'netsuite'),
('PROD-003', 'CHAIR-EXECUTIVE', 'Executive Office Chair', 'Furniture', 'Seating', 'FurnitureCo', 'SUP-002', 120.00, 199.99, 15.0, '60x60x120', TRUE, '2023-01-01', '2024-01-15 10:00:00', 'netsuite'),
('PROD-004', 'LAMP-DESK-LED', 'LED Desk Lamp', 'Furniture', 'Lighting', 'LightingInc', 'SUP-003', 25.00, 49.99, 1.2, '30x15x45', TRUE, '2023-01-01', '2024-01-15 10:00:00', 'netsuite'),
('PROD-005', 'MUG-CERAMIC', 'Ceramic Coffee Mug', 'Kitchen', 'Drinkware', 'KitchenSupplies', 'SUP-004', 5.00, 12.99, 0.3, '10x10x12', TRUE, '2023-01-01', '2024-01-15 10:00:00', 'netsuite');

-- E-commerce Products
INSERT INTO ecommerce_products VALUES
(2001, 'LAPTOP-PRO-15', 'Laptop Pro 15" - Premium Performance', '<p>High-performance laptop for professionals</p>', 'TechCorp', 'Electronics', 'laptop,computer,premium', 'active', '2023-01-01 00:00:00', '2024-01-15 10:00:00', '2023-01-01 00:00:00', '', 'shopify'),
(2002, 'MOUSE-WIRELESS', 'Wireless Optical Mouse', '<p>Ergonomic wireless mouse with precision tracking</p>', 'TechCorp', 'Electronics', 'mouse,wireless,ergonomic', 'active', '2023-01-01 00:00:00', '2024-01-15 10:00:00', '2023-01-01 00:00:00', '', 'shopify'),
(2003, 'CHAIR-EXECUTIVE', 'Executive Office Chair', '<p>Premium ergonomic office chair with lumbar support</p>', 'FurnitureCo', 'Furniture', 'chair,office,ergonomic', 'active', '2023-01-01 00:00:00', '2024-01-15 10:00:00', '2023-01-01 00:00:00', '', 'shopify'),
(2004, 'LAMP-DESK-LED', 'LED Desk Lamp', '<p>Adjustable LED desk lamp with multiple brightness levels</p>', 'LightingInc', 'Furniture', 'lamp,desk,led', 'active', '2023-01-01 00:00:00', '2024-01-15 10:00:00', '2023-01-01 00:00:00', '', 'shopify'),
(2005, 'MUG-CERAMIC', 'Ceramic Coffee Mug', '<p>High-quality ceramic coffee mug, 12oz capacity</p>', 'KitchenSupplies', 'Kitchen', 'mug,coffee,ceramic', 'active', '2023-01-01 00:00:00', '2024-01-15 10:00:00', '2023-01-01 00:00:00', '', 'shopify');

-- ERP Transactions
INSERT INTO erp_transactions VALUES
('TXN-001', 'CUST-001', '2024-01-15', 'SALE', 'LAPTOP-PRO-15', 'Laptop Pro 15"', 2, 1299.99, 2599.98, 'USD', 259.99, 0.00, 'Credit Card', 'Shipped', 'WAREHOUSE-A', 'SALES-001', 'netsuite'),
('TXN-002', 'CUST-002', '2024-01-16', 'SALE', 'MOUSE-WIRELESS', 'Wireless Mouse', 5, 29.99, 149.95, 'USD', 14.99, 10.00, 'Credit Card', 'Delivered', 'WAREHOUSE-A', 'SALES-002', 'netsuite'),
('TXN-003', 'CUST-003', '2024-01-17', 'SALE', 'CHAIR-EXECUTIVE', 'Executive Office Chair', 10, 199.99, 1999.90, 'USD', 199.99, 100.00, 'Purchase Order', 'Shipped', 'WAREHOUSE-B', 'SALES-003', 'netsuite'),
('TXN-004', 'CUST-004', '2024-01-18', 'SALE', 'LAMP-DESK-LED', 'LED Desk Lamp', 1, 49.99, 49.99, 'USD', 4.99, 0.00, 'Credit Card', 'Delivered', 'WAREHOUSE-A', 'SALES-001', 'netsuite'),
('TXN-005', 'CUST-005', '2024-01-19', 'SALE', 'MUG-CERAMIC', 'Ceramic Coffee Mug', 50, 12.99, 649.50, 'USD', 64.95, 50.00, 'Purchase Order', 'Shipped', 'WAREHOUSE-A', 'SALES-002', 'netsuite');

-- E-commerce Orders
INSERT INTO ecommerce_orders VALUES
(3001, 1001, '#1001', '2024-01-15 10:30:00', 2599.98, 2339.99, 259.99, 0.00, 'USD', 'paid', 'fulfilled', '{"address1":"123 Main St","city":"New York","state":"NY","zip":"10001"}', '{"address1":"123 Main St","city":"New York","state":"NY","zip":"10001"}', 'vip,enterprise', 'Enterprise order', 'shopify'),
(3002, 1002, '#1002', '2024-01-16 14:15:00', 139.95, 149.95, 14.99, 25.00, 'USD', 'paid', 'fulfilled', '{"address1":"456 Oak Ave","city":"San Francisco","state":"CA","zip":"94102"}', '{"address1":"456 Oak Ave","city":"San Francisco","state":"CA","zip":"94102"}', 'smb,tech', 'SMB order', 'shopify'),
(3003, 1003, '#1003', '2024-01-17 09:45:00', 1899.90, 1999.90, 199.99, 100.00, 'USD', 'paid', 'fulfilled', '{"address1":"789 Pine Rd","city":"London","state":"","zip":"SW1A 1AA"}', '{"address1":"789 Pine Rd","city":"London","state":"","zip":"SW1A 1AA"}', 'enterprise,retail', 'Enterprise retail order', 'shopify'),
(3004, 1004, '#1004', '2024-01-18 16:20:00', 49.99, 49.99, 4.99, 0.00, 'USD', 'paid', 'fulfilled', '{"address1":"321 Elm St","city":"Austin","state":"TX","zip":"73301"}', '{"address1":"321 Elm St","city":"Austin","state":"TX","zip":"73301"}', 'startup', 'Startup order', 'shopify'),
(3005, 1005, '#1005', '2024-01-19 11:30:00', 599.50, 649.50, 64.95, 50.00, 'USD', 'paid', 'fulfilled', '{"address1":"654 Maple Dr","city":"Tokyo","state":"","zip":"100-0001"}', '{"address1":"654 Maple Dr","city":"Tokyo","state":"","zip":"100-0001"}', 'enterprise,manufacturing', 'Manufacturing order', 'shopify');

-- E-commerce Order Items
INSERT INTO ecommerce_order_items VALUES
(4001, 3001, 2001, 2001, 'LAPTOP-PRO-15', 'Laptop Pro 15" - Premium Performance', '15 inch, 16GB RAM, 512GB SSD', 2, 1299.99, 2599.98, 'shopify'),
(4002, 3002, 2002, 2002, 'MOUSE-WIRELESS', 'Wireless Optical Mouse', 'Black', 5, 29.99, 149.95, 'shopify'),
(4003, 3003, 2003, 2003, 'CHAIR-EXECUTIVE', 'Executive Office Chair', 'Black Leather', 10, 199.99, 1999.90, 'shopify'),
(4004, 3004, 2004, 2004, 'LAMP-DESK-LED', 'LED Desk Lamp', 'Silver', 1, 49.99, 49.99, 'shopify'),
(4005, 3005, 2005, 2005, 'MUG-CERAMIC', 'Ceramic Coffee Mug', 'White, 12oz', 50, 12.99, 649.50, 'shopify');

-- ERP Inventory
INSERT INTO erp_inventory VALUES
('INV-001', 'PROD-001', 'WH-001', 'Warehouse A - Section 1', 25, 5, 20, 10, 30, '2024-01-10', 800.00, 'netsuite'),
('INV-002', 'PROD-002', 'WH-001', 'Warehouse A - Section 2', 150, 10, 140, 20, 100, '2024-01-10', 15.00, 'netsuite'),
('INV-003', 'PROD-003', 'WH-002', 'Warehouse B - Section 1', 15, 2, 13, 5, 20, '2024-01-10', 120.00, 'netsuite'),
('INV-004', 'PROD-004', 'WH-001', 'Warehouse A - Section 3', 80, 8, 72, 15, 50, '2024-01-10', 25.00, 'netsuite'),
('INV-005', 'PROD-005', 'WH-001', 'Warehouse A - Section 4', 300, 25, 275, 50, 200, '2024-01-10', 5.00, 'netsuite');

-- Third Party Inventory
INSERT INTO third_party_inventory VALUES
('3PL-001', 'LAPTOP-PRO-15', '3PL-WH-01', 'Third Party Warehouse 1', 10, 2, 0, '2024-01-15 10:00:00', 'v2.1', '3pl_system'),
('3PL-002', 'MOUSE-WIRELESS', '3PL-WH-01', 'Third Party Warehouse 1', 50, 5, 1, '2024-01-15 10:00:00', 'v2.1', '3pl_system'),
('3PL-003', 'CHAIR-EXECUTIVE', '3PL-WH-02', 'Third Party Warehouse 2', 5, 1, 0, '2024-01-15 10:00:00', 'v2.1', '3pl_system'),
('3PL-004', 'LAMP-DESK-LED', '3PL-WH-01', 'Third Party Warehouse 1', 30, 3, 0, '2024-01-15 10:00:00', 'v2.1', '3pl_system'),
('3PL-005', 'MUG-CERAMIC', '3PL-WH-01', 'Third Party Warehouse 1', 200, 20, 2, '2024-01-15 10:00:00', 'v2.1', '3pl_system');

-- Email Campaigns
INSERT INTO email_campaigns VALUES
('EMAIL-001', 'Q1 Product Launch', 'Product Launch', 'New Laptop Pro 15" - Available Now!', 'marketing@company.com', '2024-01-15 10:00:00', 10000, 2500, 500, 100, 50, 25.00, 5.00, 1.00, 'email_platform'),
('EMAIL-002', 'Customer Appreciation', 'Customer Retention', 'Thank You for Your Business', 'marketing@company.com', '2024-01-16 14:00:00', 5000, 1500, 300, 25, 10, 30.00, 6.00, 0.50, 'email_platform'),
('EMAIL-003', 'Holiday Sale', 'Promotional', 'Up to 50% Off - Limited Time!', 'marketing@company.com', '2024-01-17 09:00:00', 15000, 3750, 750, 150, 75, 25.00, 5.00, 1.00, 'email_platform');

-- Email Campaign Recipients
INSERT INTO email_campaign_recipients VALUES
('REC-001', 'EMAIL-001', 'john.doe@acme.com', 'CONT-001', '2024-01-15 10:00:00', '2024-01-15 11:30:00', '2024-01-15 12:15:00', FALSE, FALSE, 'email_platform'),
('REC-002', 'EMAIL-001', 'sarah.smith@techstart.com', 'CONT-002', '2024-01-15 10:00:00', '2024-01-15 10:45:00', NULL, FALSE, FALSE, 'email_platform'),
('REC-003', 'EMAIL-002', 'john.doe@acme.com', 'CONT-001', '2024-01-16 14:00:00', '2024-01-16 15:20:00', '2024-01-16 16:10:00', FALSE, FALSE, 'email_platform'),
('REC-004', 'EMAIL-003', 'emily.davis@startupxyz.com', 'CONT-004', '2024-01-17 09:00:00', NULL, NULL, TRUE, FALSE, 'email_platform');

-- Social Media Campaigns
INSERT INTO social_campaigns VALUES
('SOCIAL-001', 'facebook', 'Q1 Brand Awareness', 'Brand Awareness', '2024-01-01', '2024-01-31', 5000.00, 4850.00, 100000, 2500, 125, 2.50, 1.94, 48.50, 'social_platform'),
('SOCIAL-002', 'instagram', 'Product Showcase', 'Product Promotion', '2024-01-15', '2024-01-30', 3000.00, 2950.00, 75000, 1500, 75, 2.00, 1.97, 39.33, 'social_platform'),
('SOCIAL-003', 'linkedin', 'B2B Lead Generation', 'Lead Generation', '2024-01-10', '2024-01-25', 2000.00, 1980.00, 25000, 500, 25, 2.00, 3.96, 79.20, 'social_platform');

-- Product Reviews
INSERT INTO product_reviews VALUES
('REV-001', 2001, 'john.doe@acme.com', 5, 'Excellent Performance', 'This laptop exceeded my expectations. Fast, reliable, and perfect for work.', '2024-01-20 14:30:00', TRUE, 12, 'ecommerce'),
('REV-002', 2002, 'sarah.smith@techstart.com', 4, 'Great Wireless Mouse', 'Good quality mouse, battery life could be better but overall satisfied.', '2024-01-21 10:15:00', TRUE, 8, 'ecommerce'),
('REV-003', 2003, 'mike.johnson@globalretail.com', 3, 'Comfortable but Expensive', 'The chair is comfortable but seems overpriced for what you get.', '2024-01-22 16:45:00', TRUE, 5, 'ecommerce'),
('REV-004', 2004, 'emily.davis@startupxyz.com', 5, 'Perfect Desk Lamp', 'Exactly what I needed for my home office. Great lighting and adjustable.', '2024-01-23 11:20:00', TRUE, 15, 'ecommerce'),
('REV-005', 2005, 'david.wilson@mfgcorp.com', 4, 'Good Quality Mugs', 'Nice ceramic mugs, good for office use. Would buy again.', '2024-01-24 13:10:00', TRUE, 6, 'ecommerce');

-- Support Tickets
INSERT INTO support_tickets VALUES
('TICKET-001', 'john.doe@acme.com', 'CUST-001', 'T-2024-001', 'Laptop not charging', 'My laptop stopped charging after 2 weeks of use. Need assistance.', 'High', 'Resolved', 'Hardware', 'Support-001', '2024-01-18 09:00:00', '2024-01-19 14:30:00', 29, 4, 'support_system'),
('TICKET-002', 'sarah.smith@techstart.com', 'CUST-002', 'T-2024-002', 'Mouse connectivity issues', 'Wireless mouse keeps disconnecting randomly.', 'Medium', 'Open', 'Hardware', 'Support-002', '2024-01-20 11:15:00', NULL, NULL, NULL, 'support_system'),
('TICKET-003', 'mike.johnson@globalretail.com', 'CUST-003', 'T-2024-003', 'Bulk order inquiry', 'Interested in placing a large order for office furniture.', 'Low', 'Resolved', 'Sales', 'Sales-001', '2024-01-21 15:30:00', '2024-01-22 10:00:00', 18, 5, 'support_system');

-- Accounting Transactions
INSERT INTO accounting_transactions VALUES
('ACC-001', '2024-01-15', 'Sale', 'Accounts Receivable', 'Asset', 'Sale of Laptop Pro 15"', 2599.98, 2599.98, 0.00, 'TXN-001', 'CUST-001', NULL, 'Sales', 'North America', 'quickbooks'),
('ACC-002', '2024-01-15', 'Sale', 'Revenue', 'Income', 'Sale of Laptop Pro 15"', 2599.98, 0.00, 2599.98, 'TXN-001', 'CUST-001', NULL, 'Sales', 'North America', 'quickbooks'),
('ACC-003', '2024-01-16', 'Sale', 'Accounts Receivable', 'Asset', 'Sale of Wireless Mouse', 149.95, 149.95, 0.00, 'TXN-002', 'CUST-002', NULL, 'Sales', 'North America', 'quickbooks'),
('ACC-004', '2024-01-16', 'Sale', 'Revenue', 'Income', 'Sale of Wireless Mouse', 149.95, 0.00, 149.95, 'TXN-002', 'CUST-002', NULL, 'Sales', 'North America', 'quickbooks'),
('ACC-005', '2024-01-17', 'Expense', 'Cost of Goods Sold', 'Expense', 'Cost of Executive Chairs', 1200.00, 1200.00, 0.00, 'TXN-003', NULL, 'SUP-002', 'Cost of Sales', 'North America', 'quickbooks');

-- Shipping Orders
INSERT INTO shipping_orders VALUES
('SHIP-001', 'TXN-001', '1Z999AA1234567890', 'UPS', 'Ground', '2024-01-16 14:30:00', '2024-01-18 10:15:00', 25.00, 5.0, '35x24x8', 'Delivered', 'shipping_system'),
('SHIP-002', 'TXN-002', '1Z999AA1234567891', 'FedEx', 'Express', '2024-01-17 09:15:00', '2024-01-18 14:30:00', 15.00, 0.5, '12x6x3', 'Delivered', 'shipping_system'),
('SHIP-003', 'TXN-003', '1Z999AA1234567892', 'UPS', 'Freight', '2024-01-18 16:45:00', '2024-01-22 11:20:00', 150.00, 150.0, '60x60x120', 'Delivered', 'shipping_system'),
('SHIP-004', 'TXN-004', '1Z999AA1234567893', 'USPS', 'Priority', '2024-01-19 10:30:00', '2024-01-21 15:45:00', 8.00, 1.2, '30x15x45', 'Delivered', 'shipping_system'),
('SHIP-005', 'TXN-005', '1Z999AA1234567894', 'UPS', 'Ground', '2024-01-20 13:15:00', '2024-01-23 09:30:00', 45.00, 15.0, '10x10x12', 'In Transit', 'shipping_system');

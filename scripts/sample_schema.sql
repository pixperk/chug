-- CHUG Sample Schema
-- Creates 4 sample tables with 1000 rows each for testing multi-table ingestion

-- Drop existing tables
DROP TABLE IF EXISTS users, products, orders, events CASCADE;

-- Table 1: Users
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Products
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    total_amount DECIMAL(10,2),
    order_status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: Events
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id INTEGER,
    event_data TEXT,
    severity VARCHAR(20) DEFAULT 'info',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 1000 users
INSERT INTO users (username, email, status, created_at, updated_at)
SELECT
    'user_' || i,
    'user' || i || '@example.com',
    CASE (random() * 3)::int
        WHEN 0 THEN 'active'
        WHEN 1 THEN 'inactive'
        WHEN 2 THEN 'pending'
        ELSE 'suspended'
    END,
    NOW() - (random() * interval '365 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 1000) AS i;

-- Insert 1000 products
INSERT INTO products (name, category, price, stock, created_at, updated_at)
SELECT
    CASE (random() * 5)::int
        WHEN 0 THEN 'Product_' || i
        WHEN 1 THEN 'Gadget_' || i
        WHEN 2 THEN 'Tool_' || i
        WHEN 3 THEN 'Device_' || i
        WHEN 4 THEN 'Item_' || i
        ELSE 'Thing_' || i
    END,
    CASE (random() * 4)::int
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Furniture'
        WHEN 2 THEN 'Stationery'
        WHEN 3 THEN 'Accessories'
        ELSE 'Clothing'
    END,
    (random() * 1000 + 10)::decimal(10,2),
    (random() * 500)::int,
    NOW() - (random() * interval '365 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 1000) AS i;

-- Insert 1000 orders
INSERT INTO orders (user_id, product_id, quantity, total_amount, order_status, created_at, updated_at)
SELECT
    (random() * 999 + 1)::int,
    (random() * 999 + 1)::int,
    (random() * 10 + 1)::int,
    (random() * 5000 + 10)::decimal(10,2),
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'completed'
        ELSE 'cancelled'
    END,
    NOW() - (random() * interval '180 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 1000) AS i;

-- Insert 1000 events
INSERT INTO events (event_type, user_id, event_data, severity, created_at, updated_at)
SELECT
    CASE (random() * 7)::int
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'purchase'
        WHEN 3 THEN 'page_view'
        WHEN 4 THEN 'cart_add'
        WHEN 5 THEN 'search'
        WHEN 6 THEN 'wishlist_add'
        ELSE 'profile_update'
    END,
    (random() * 999 + 1)::int,
    '{"timestamp": "' || NOW() || '", "session_id": "' || substr(md5(random()::text), 1, 16) || '", "ip": "192.168.' || (random()*255)::int || '.' || (random()*255)::int || '"}',
    CASE (random() * 4)::int
        WHEN 0 THEN 'info'
        WHEN 1 THEN 'warning'
        WHEN 2 THEN 'error'
        WHEN 3 THEN 'debug'
        ELSE 'critical'
    END,
    NOW() - (random() * interval '90 days'),
    NOW() - (random() * interval '7 days')
FROM generate_series(1, 1000) AS i;

-- Create indexes on updated_at columns for CDC
CREATE INDEX idx_users_updated ON users(updated_at);
CREATE INDEX idx_products_updated ON products(updated_at);
CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE INDEX idx_events_updated ON events(updated_at);

-- Display summary
SELECT
    'Sample schema created successfully!' as status;

SELECT
    'users' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('users')) as size
FROM users
UNION ALL
SELECT 'products', COUNT(*), pg_size_pretty(pg_total_relation_size('products')) FROM products
UNION ALL
SELECT 'orders', COUNT(*), pg_size_pretty(pg_total_relation_size('orders')) FROM orders
UNION ALL
SELECT 'events', COUNT(*), pg_size_pretty(pg_total_relation_size('events')) FROM events
ORDER BY table_name;

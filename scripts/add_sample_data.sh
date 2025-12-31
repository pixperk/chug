#!/bin/bash
set -e

# Script to add more sample data to orders and events tables

PG_URL="${LOCAL_PG:-postgresql://chugger:secret@localhost:5433/chugdb}"
ORDERS_COUNT="${1:-100}"
EVENTS_COUNT="${2:-500}"

echo "Adding $ORDERS_COUNT orders and $EVENTS_COUNT events to database..."

psql "$PG_URL" <<EOF
-- Add more orders (with current timestamp for CDC detection)
INSERT INTO orders (user_id, product_id, quantity, total_amount, order_status, created_at, updated_at)
SELECT
    (random() * 15 + 1)::int,
    (random() * 12 + 1)::int,
    (random() * 5 + 1)::int,
    (random() * 1000 + 10)::decimal(10,2),
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'completed'
        ELSE 'cancelled'
    END,
    NOW(),
    NOW()
FROM generate_series(1, $ORDERS_COUNT);

-- Add more events (with current timestamp for CDC detection)
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
    (random() * 15 + 1)::int,
    '{"timestamp": "' || NOW() || '", "session_id": "' || substr(md5(random()::text), 1, 16) || '", "ip": "192.168.' || (random()*255)::int || '.' || (random()*255)::int || '"}',
    CASE (random() * 4)::int
        WHEN 0 THEN 'info'
        WHEN 1 THEN 'warning'
        WHEN 2 THEN 'error'
        ELSE 'debug'
    END,
    NOW(),
    NOW()
FROM generate_series(1, $EVENTS_COUNT);

-- Show updated counts
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
EOF

echo "Sample data added successfully!"

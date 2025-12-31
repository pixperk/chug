#!/bin/bash
set -e

PG_URL="${LOCAL_PG:-postgresql://chugger:secret@localhost:5433/chugdb}"
ROWS_PER_TABLE="${1:-10000000}"  # 10M default

echo "Generating $ROWS_PER_TABLE rows per table (3 tables = $((ROWS_PER_TABLE * 3)) total)..."
echo "This will take a while..."

start_time=$(date +%s)

psql "$PG_URL" <<EOF
-- Generate large datasets efficiently
BEGIN;

-- Events: $ROWS_PER_TABLE rows
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
    (random() * 100000 + 1)::int,
    '{"session": "' || substr(md5(random()::text), 1, 16) || '"}',
    CASE (random() * 3)::int
        WHEN 0 THEN 'info'
        WHEN 1 THEN 'warning'
        WHEN 2 THEN 'error'
        ELSE 'debug'
    END,
    NOW() - (random() * interval '365 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, $ROWS_PER_TABLE);

-- Orders: $ROWS_PER_TABLE rows
INSERT INTO orders (user_id, product_id, quantity, total_amount, order_status, created_at, updated_at)
SELECT
    (random() * 100000 + 1)::int,
    (random() * 1000 + 1)::int,
    (random() * 10 + 1)::int,
    (random() * 5000 + 10)::decimal(10,2),
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'completed'
        ELSE 'cancelled'
    END,
    NOW() - (random() * interval '365 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, $ROWS_PER_TABLE);

-- Users: $ROWS_PER_TABLE rows
INSERT INTO users (username, email, status, created_at, updated_at)
SELECT
    'user_' || i,
    'user' || i || '@example.com',
    CASE (random() * 2)::int WHEN 0 THEN 'active' WHEN 1 THEN 'inactive' ELSE 'pending' END,
    NOW() - (random() * interval '365 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, $ROWS_PER_TABLE) i;

COMMIT;

-- Show stats
SELECT
    'events' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('events')) as size
FROM events
UNION ALL
SELECT 'orders', COUNT(*), pg_size_pretty(pg_total_relation_size('orders')) FROM orders
UNION ALL
SELECT 'users', COUNT(*), pg_size_pretty(pg_total_relation_size('users')) FROM users
ORDER BY table_name;
EOF

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "Generated $((ROWS_PER_TABLE * 3)) total rows in $duration seconds"
echo "Rate: $(( (ROWS_PER_TABLE * 3) / duration )) rows/sec"

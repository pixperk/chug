#!/bin/bash
set -e

# Script to clean up all tables and data from local PostgreSQL and ClickHouse

PG_URL="${LOCAL_PG:-postgresql://chugger:secret@localhost:5433/chugdb}"
CH_URL="${LOCAL_CLICKHOUSE:-http://localhost:9000}"

echo "Cleaning up databases..."
echo ""

# Clean PostgreSQL
echo "Cleaning PostgreSQL tables..."
psql "$PG_URL" <<EOF
-- Drop all application tables
DROP TABLE IF EXISTS users, products, orders, events CASCADE;
DROP TABLE IF EXISTS bench_data, bench_data_cdc, bench_data_multi_1, bench_data_multi_2, bench_data_multi_3 CASCADE;

-- Show remaining tables
SELECT
    schemaname,
    tablename
FROM pg_tables
WHERE schemaname = 'public';
EOF

echo "PostgreSQL cleaned"
echo ""

# Clean ClickHouse
echo "Cleaning ClickHouse tables..."
docker exec chug_clickhouse clickhouse-client --query "SHOW TABLES" | while read table; do
    if [ ! -z "$table" ]; then
        echo "  Dropping table: $table"
        docker exec chug_clickhouse clickhouse-client --query "DROP TABLE IF EXISTS $table"
    fi
done

echo "ClickHouse cleaned"
echo ""
echo "All databases cleaned successfully!"

#!/bin/bash

set -e

PG_URL="${LOCAL_PG:-postgresql://chugger:secret@localhost:5433/chugdb}"
UPDATE_COUNT="${1:-50}"

echo "Updating $UPDATE_COUNT random event rows..."

psql "$PG_URL" <<EOF
UPDATE events
SET
    event_data = '{"updated": true, "timestamp": "' || NOW() || '"}',
    severity = CASE severity
        WHEN 'info' THEN 'warning'
        WHEN 'warning' THEN 'error'
        WHEN 'error' THEN 'critical'
        ELSE 'info'
    END,
    updated_at = NOW()
WHERE id IN (
    SELECT id FROM events
    ORDER BY RANDOM()
    LIMIT $UPDATE_COUNT
)
RETURNING id, event_type, severity;
EOF

echo "Updated $UPDATE_COUNT events"

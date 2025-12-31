# CHUG

High-performance ETL pipeline for PostgreSQL to ClickHouse.

[![Go Report Card](https://goreportcard.com/badge/github.com/pixperk/chug)](https://goreportcard.com/report/github.com/pixperk/chug)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pixperk/chug)](https://github.com/pixperk/chug)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

CHUG streams data from PostgreSQL to ClickHouse with optimized performance and constant memory usage.

**Key Features**

- Streaming architecture with constant memory footprint
- Connection pooling for both source and target
- Parallel batch insertion with 4-worker pool
- Automatic schema mapping and type conversion
- Change data capture via delta column polling
- Zero-config quick start with sensible defaults

## Quick Start

Get started with CHUG in 3 steps using Docker:

### 1. Start Local Databases

```bash
# Start PostgreSQL and ClickHouse
docker-compose up -d

# Verify containers are running
docker ps
```

### 2. Populate Sample Data

```bash
# Create 4 sample tables with sample data (easiest way!)
make hydrate

# Or manually:
# psql "postgresql://chugger:secret@localhost:5433/chugdb" < scripts/sample_schema.sql

# Add more data to existing tables
make add-data ORDERS_COUNT=100 EVENTS_COUNT=500
```

### 3. Run Multi-Table Ingestion

```bash
# Build CHUG
go build -o chug

# Generate and edit config
./chug sample-config
# Edit .chug.yaml with your table settings

# Run ingestion (uses .chug.yaml)
./chug ingest

# Or with verbose logging to see everything
./chug ingest --verbose
```

**Database Management:**
```bash
# Hydrate with sample data (creates 4 tables)
make hydrate

# Add more sample data to orders and events
make add-data ORDERS_COUNT=200 EVENTS_COUNT=1000

# Clean all tables and start fresh
make clean-db
```

That's it! Your data is now streaming from PostgreSQL to ClickHouse in parallel.

**Ultra-Quick Start (3 commands):**
```bash
docker-compose up -d && make hydrate && ./chug ingest
```

## Architecture

```mermaid
flowchart LR
    PG[(PostgreSQL)] -->|Stream| Pool1[Connection Pool]
    Pool1 -->|Rows| Chan[Channel Buffer]
    Chan -->|Batch| Workers[4 Workers]
    Workers -->|Parallel Insert| Pool2[Connection Pool]
    Pool2 --> CH[(ClickHouse)]

    style PG fill:#C62828,stroke:#B71C1C,color:#FFF
    style Pool1 fill:#1565C0,stroke:#0D47A1,color:#FFF
    style Chan fill:#6A1B9A,stroke:#4A148C,color:#FFF
    style Workers fill:#2E7D32,stroke:#1B5E20,color:#FFF
    style Pool2 fill:#1565C0,stroke:#0D47A1,color:#FFF
    style CH fill:#EF6C00,stroke:#E65100,color:#FFF
```

**Pipeline Flow:**

1. Connection pools eliminate per-query overhead
2. Streaming extractor fetches rows via channels
3. Batch builder accumulates configurable batch size (default: 500)
4. 4 parallel workers insert batches concurrently
5. Automatic schema creation in ClickHouse

**Performance:**

| Optimization | Impact |
|--------------|--------|
| Connection pooling | 10-20% faster |
| Streaming | Constant memory |
| 4 parallel workers | 2-5x throughput |
| Indexed polling | 100-1000x faster CDC |

## Benchmarks

Comprehensive performance testing with percentile metrics (p50, p95, p99) for realistic analysis.

### Local Performance (Docker)

| Operation | p50 Latency | Throughput | Batch Size |
|-----------|-------------|------------|------------|
| Extract 10K rows | 8.8ms | 111 ops/sec | - |
| Extract 100K rows | 85ms | 11.6 ops/sec | - |
| Insert 10K rows | 11.9ms | 84 ops/sec | 1000 |
| Insert 10K rows | 17ms | 59 ops/sec | 500 |
| Insert 50K rows | 39.8ms | 24 ops/sec | 2000 |
| CDC (10K changes) | 8.7ms | 110 ops/sec | - |

### Remote Performance (Cloud - Asia Pacific)

| Operation | p50 Latency | Throughput | vs Local |
|-----------|-------------|------------|----------|
| Extract 10K rows | 482ms | 2.11 ops/sec | **55x slower** |
| Extract 100K rows | 3.36s | 0.25 ops/sec | **40x slower** |
| Insert 10K rows (batch 500) | 443ms | 2.25 ops/sec | **26x slower** |
| Insert 10K rows (batch 1000) | 269ms | 3.72 ops/sec | **23x slower** |
| Insert 50K rows (batch 2000) | 813ms | 1.15 ops/sec | **20x slower** |
| CDC (10K changes) | 483ms | 2.08 ops/sec | **55x slower** |

**Key Insights:**
- Network latency dominates remote performance (20-55x slower)
- Insertions scale better than extractions (20-26x vs 40-55x slowdown)
- Larger batches amortize network overhead (batch 1000 is 65% faster than batch 500)
- CDC performance matches extraction (both ~55x slower for 10K rows)

### Running Benchmarks

**Setup benchmark tables (100K rows):**
```bash
make bench-setup-local   # Local Docker
make bench-setup-remote  # Remote cloud
```

**Run benchmarks:**
```bash
make bench-local         # All benchmarks (local)
make bench-remote        # All benchmarks (remote)
make bench-both          # Compare local vs remote
make bench-extract       # Extraction only
make bench-insert        # Insertion only
make bench-cdc           # CDC only
```

**Custom configuration:**
```bash
BENCH_ITERATIONS=50 make bench-local
BENCH_TABLE=my_table make bench-extract
```

See [bench/README.md](bench/README.md) for detailed benchmarking documentation.

## Installation

### Prerequisites

- Go 1.20+
- PostgreSQL 12+
- ClickHouse 21+

### Build

```bash
git clone https://github.com/pixperk/chug.git
cd chug
go build -o chug
sudo mv chug /usr/local/bin/
```

## Configuration

**YAML config (recommended)** - Simplest way to manage settings:

```bash
# Generate sample config
chug sample-config

# Edit .chug.yaml with your settings
# Then run with just:
chug ingest
```

**Example `.chug.yaml`:**

```yaml
pg_url: "postgres://user:password@localhost:5432/mydb"
ch_url: "http://localhost:9000"
table: "users"
limit: 0
batch_size: 500

polling:
  enabled: false
  delta_column: "updated_at"
  interval_seconds: 30
```

**Multi-table config:**

```yaml
pg_url: "postgres://user:password@localhost:5432/mydb"
ch_url: "http://localhost:9000"
batch_size: 500   # Global default

tables:
  - name: users
    batch_size: 1000   # Override for this table

  - name: orders
    limit: 5000
    polling:
      enabled: true
      delta_column: "updated_at"
      interval_seconds: 60

  - name: products
    # Uses global defaults
```

## Usage

### Easy Way: YAML Config

```bash
# Create config
chug sample-config

# Run ingestion
chug ingest                    # Uses .chug.yaml in current directory
chug ingest --config my.yaml   # Use specific config file
```

### Alternative: CLI Flags

For quick one-off runs without config files:

```bash
# Test connections
chug connect --pg-url <pg-url> --ch-url <ch-url>

# Ingest single table
chug ingest \
  --pg-url "postgres://user:pass@host:port/db" \
  --ch-url "http://host:port" \
  --table "users" \
  --limit 0 \
  --batch-size 500
```

### Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--pg-url` | PostgreSQL connection string | - |
| `--ch-url` | ClickHouse URL | - |
| `--table` | Table name | - |
| `--limit` | Max rows (0 = unlimited) | 1000 |
| `--batch-size` | Rows per batch | 500 |
| `--config` | YAML config file path | .chug.yaml |
| `--poll` | Enable CDC polling | false |
| `--poll-delta` | Delta column name | - |
| `--poll-interval` | Poll interval (seconds) | - |
| `--verbose`, `-v` | Enable verbose logging | false |

## Change Data Capture (CDC)

CHUG implements polling-based CDC with automatic update deduplication using ClickHouse ReplacingMergeTree.

**What CDC Detects:**
- New row INSERTs (with delta_column >= last_seen)
- Row UPDATEs (deduplicates based on primary key)
- Row DELETEs are NOT supported (see design decision below)

### Architecture

```mermaid
graph TB
    subgraph PostgreSQL
        PG[(PostgreSQL)]
        PG_DATA[Table with updated_at]
    end

    subgraph CHUG
        INIT[Initial Full Sync]
        DETECT[Primary Key Detection]
        POLL[Polling Loop<br/>Every N seconds]
        QUERY["SELECT * FROM table<br/>WHERE updated_at > last_seen"]
    end

    subgraph ClickHouse
        CH[(ClickHouse)]
        RMT[ReplacingMergeTree<br/>with _dedup_key]
        HASH[Hash PK Columns<br/>cityHash64 tuple id]
        DEDUP[Background Deduplication<br/>Keep latest updated_at]
        FINAL[Query with FINAL<br/>for deduplicated view]
    end

    PG_DATA --> INIT
    INIT --> DETECT
    DETECT --> |"Query PG for<br/>PRIMARY KEY"| RMT
    INIT --> |"1000 rows"| RMT

    POLL --> QUERY
    QUERY --> |"WHERE updated_at > X"| PG_DATA
    PG_DATA --> |"50 updated rows"| RMT

    RMT --> HASH
    HASH --> |"Same hash = same row"| DEDUP
    DEDUP --> |"1000 deduplicated rows"| FINAL

    style INIT fill:#2E7D32,stroke:#1B5E20,color:#FFF
    style POLL fill:#1565C0,stroke:#0D47A1,color:#FFF
    style DEDUP fill:#6A1B9A,stroke:#4A148C,color:#FFF
    style FINAL fill:#F57C00,stroke:#E65100,color:#FFF
```

### Quick Start with YAML

```yaml
tables:
  - name: "events"
    polling:
      enabled: true
      delta_column: "updated_at"
      interval_seconds: 60
```

Then run:
```bash
./chug ingest
```

### How It Works

**1. Initial Sync + Primary Key Detection**
- Performs full table ingestion
- Queries PostgreSQL `information_schema` for primary key columns
- Creates ClickHouse table with `ReplacingMergeTree` engine
- Adds `_dedup_key` column: `cityHash64(tuple(pk_columns))`

**2. Polling Loop**
- Tracks MAX(delta_column) as `last_seen`
- Every N seconds, queries: `SELECT * WHERE delta_column > last_seen`
- Inserts new/updated rows to ClickHouse
- Updates `last_seen` to latest timestamp

**3. Update Deduplication**
- PostgreSQL UPDATE triggers `updated_at` change
- Row gets re-inserted to ClickHouse with new data
- ReplacingMergeTree detects same primary key hash
- Keeps version with latest `updated_at` timestamp
- Query with `FINAL` to see deduplicated results

**Example Flow:**
```sql
-- PostgreSQL: Update row id=5
UPDATE events SET severity='critical', updated_at=NOW() WHERE id=5;

-- ClickHouse: Two versions temporarily stored
-- Old: hash(id=5), severity='warning', updated_at='10:00'
-- New: hash(id=5), severity='critical', updated_at='10:05'

-- ReplacingMergeTree deduplication
SELECT * FROM events FINAL;  -- Returns 1 row with latest data
```

### Configuration

**CLI Flags:**
```bash
chug ingest \
  --table "events" \
  --poll \
  --poll-delta "updated_at" \
  --poll-interval 60
```

**YAML Config (Recommended):**
```yaml
tables:
  - name: "events"
    polling:
      enabled: true
      delta_column: "updated_at"
      interval_seconds: 60
```

### Requirements

**Delta Column:**
- Must be monotonically increasing (timestamp, serial)
- Indexed automatically by CHUG for performance
- For UPDATE detection, add trigger:

```sql
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_events_timestamp
BEFORE UPDATE ON events
FOR EACH ROW EXECUTE FUNCTION update_timestamp();
```

### Primary Key Detection

CHUG automatically detects primary keys from PostgreSQL:
- Queries `information_schema.table_constraints`
- Supports single and composite primary keys
- Falls back to all columns if no PK detected

### Testing CDC

**Test Updates (Deduplication):**
```bash
# Update some rows in PostgreSQL
make update-data UPDATE_COUNT=100

# Verify deduplication
docker exec chug_clickhouse clickhouse-client --query \
  "SELECT COUNT(*) FROM events FINAL;"
```

**Test Inserts:**
```bash
# Add new rows to PostgreSQL
make add-data EVENTS_COUNT=50

# CDC will detect and sync within interval seconds
# Verify rows synced
docker exec chug_clickhouse clickhouse-client --query \
  "SELECT COUNT(*) FROM events FINAL;"
```

**Important:** New rows MUST have `updated_at = NOW()` or later than the last synced timestamp. Rows with past timestamps will NOT be detected.

### Design Decision: No Delete Support

CHUG is designed as an **append-only CDC pipeline** optimized for analytics workloads. Row deletions in PostgreSQL are NOT propagated to ClickHouse.

**Rationale:**
- ClickHouse is typically used for analytics where historical data is valuable
- Deleted rows often represent important events (canceled orders, removed users) worth analyzing
- Delete handling adds significant complexity (audit tables, triggers, different table engines)
- Most production ETL tools (Airbyte, Fivetran) use append-only models for time-series data

**If you need delete tracking:**
- Implement soft deletes in PostgreSQL (add `deleted_at TIMESTAMP` column)
- Set `deleted_at = NOW()` instead of DELETE
- Updates will sync automatically via CDC
- Query with `WHERE deleted_at IS NULL` for active rows

## Type Mapping

| PostgreSQL | ClickHouse |
|------------|------------|
| INTEGER, SERIAL | Int32 |
| BIGINT, BIGSERIAL | Int64 |
| SMALLINT | Int16 |
| DOUBLE PRECISION | Float64 |
| NUMERIC | Decimal |
| VARCHAR, TEXT | String |
| BOOLEAN | UInt8 |
| TIMESTAMP | DateTime |
| DATE | Date |
| UUID | UUID |
| JSONB | String |

## Development

### Project Structure

```
chug/
├── cmd/            # CLI commands
├── internal/
│   ├── config/    # Configuration
│   ├── db/        # Connection pools
│   ├── etl/       # ETL pipeline
│   ├── logx/      # Logging
│   ├── poller/    # CDC
│   └── ui/        # Terminal UI
└── main.go
```

### Build & Test

```bash
go build -o chug
go test ./...
```

### Local Development

```bash
docker-compose up -d
go run main.go ingest --pg-url "..." --ch-url "..." --table "test"
docker-compose logs -f
docker-compose down -v
```

## Contributing

1. Fork repository
2. Create feature branch: `git checkout -b feature/name`
3. Make changes
4. Run tests: `go test ./...`
5. Commit: `git commit -m "feat: description"`
6. Push and open PR

## License

MIT License. See [LICENSE](LICENSE).

---

Built with [pgx](https://github.com/jackc/pgx), [ClickHouse Go](https://github.com/ClickHouse/clickhouse-go), [Cobra](https://github.com/spf13/cobra), [Zap](https://github.com/uber-go/zap).

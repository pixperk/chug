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

| Operation | p50 | p95 | p99 | Throughput | Batch |
|-----------|-----|-----|-----|------------|-------|
| Extract 1K rows | 1.41ms | 3.01ms | 3.01ms | 675 ops/sec | - |
| Extract 10K rows | 8.47ms | 10.4ms | 10.4ms | 117 ops/sec | - |
| Extract 100K rows | 79.3ms | 85.2ms | 85.2ms | 12.6 ops/sec | - |
| Insert 10K rows | 11.0ms | 12.3ms | 12.3ms | **91 ops/sec** | 1000 |
| Insert 10K rows | 15.4ms | 33.6ms | 33.6ms | 62 ops/sec | 500 |
| Insert 50K rows | 37.0ms | 39.7ms | 39.7ms | 27 ops/sec | 2000 |
| CDC (1K changes) | 1.15ms | 1.98ms | 1.98ms | 871 ops/sec | - |
| CDC (10K changes) | 8.08ms | 9.70ms | 9.70ms | 124 ops/sec | - |
| **Multi-table (3×10K)** | **67.2ms** | **78.5ms** | **78.5ms** | **14.8 ops/sec** | 500 |

### Remote Performance (Cloud - Asia Pacific)

**Infrastructure:** Neon PostgreSQL (Singapore) → ClickHouse Cloud (Mumbai)

| Operation | p50 | p95 | p99 | Throughput | vs Local |
|-----------|-----|-----|-----|------------|----------|
| Extract 1K rows | 151ms | 171ms | 171ms | 6.6 ops/sec | **107x slower** |
| Extract 10K rows | 471ms | 523ms | 523ms | 2.2 ops/sec | **55x slower** |
| Extract 100K rows | 3.15s | 3.32s | 3.32s | 0.33 ops/sec | **39x slower** |
| Insert 10K (batch 500) | 484ms | 703ms | 703ms | 2.0 ops/sec | **31x slower** |
| Insert 10K (batch 1000) | 275ms | 500ms | 500ms | 3.4 ops/sec | **25x slower** |
| Insert 50K (batch 2000) | 686ms | 923ms | 923ms | 1.4 ops/sec | **19x slower** |
| CDC (1K changes) | 141ms | 143ms | 143ms | 7.1 ops/sec | **122x slower** |
| CDC (10K changes) | 455ms | 510ms | 510ms | 2.2 ops/sec | **56x slower** |
| **Multi-table (3×10K)** | **1.07s** | **2.77s** | **2.77s** | **0.85 ops/sec** | **17x slower** |

**Key Insights:**
- **Tail latencies (p95/p99)** show consistent performance - most operations have tight distributions
- Network latency dominates remote performance (17-122x slower depending on operation size)
- Smaller operations suffer most from round-trip overhead (1K: 107-122x vs 100K: 39x)
- Larger batches amortize network costs better (batch 1000 is 25% faster than batch 500)
- Multi-table parallel execution helps mitigate cross-region latency (only 17x slower)
- CDC performance scales with data size (1K: 122x slower, 10K: 56x slower)
- Remote p95 can be **2.6x worse** than p50 (multi-table: 1.07s → 2.77s) due to network variance

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

### Production Scale Testing

End-to-end test with 30M rows across 3 tables, including full CDC pipeline:

**Test Configuration:**
- 3 tables (events, orders, users)
- 10M rows per table
- Batch size: 5000
- Polling interval: 3 seconds
- Environment: Local Docker (PostgreSQL + ClickHouse)

**Initial Full Sync:**
| Metric | Value |
|--------|-------|
| Total rows | 30,013,000 |
| Duration | 3m 26s (206s) |
| Throughput | **145,694 rows/sec** |
| Database size | ~4.8 GB total |
| Events table | 1.7 GB (10M rows) |
| Orders table | 1.5 GB (10M rows) |
| Users table | 1.65 GB (10M rows) |

**CDC Performance:**
| Metric | Value |
|--------|-------|
| Detection latency | <3 seconds (polling interval) |
| Sync latency | ~1 second |
| Test insert | 10,000 rows (3,333 orders + 6,667 events) |
| Sync result | All rows detected and synced in single cycle |

**Key Insights:**
- Streaming architecture maintains constant memory usage even with 30M+ rows
- Parallel ingestion across 3 tables achieves 145k rows/sec on local Docker
- CDC polling efficiently detects and syncs changes within seconds
- ReplacingMergeTree handles deduplication automatically via primary key hash

**Running Scale Tests:**
```bash
# Generate 30M rows (10M per table)
make hydrate
./scripts/generate_large_dataset.sh 10000000

# Run full sync with CDC enabled
./chug ingest --config .chug.scale-test.yaml

# Test CDC with continuous inserts
./scripts/add_sample_data.sh 5000 5000  # Add 10k rows
```

### Remote Cloud Performance

Cross-region cloud deployment test (Neon PostgreSQL → ClickHouse Cloud):

**Infrastructure:**
- Source: Neon PostgreSQL (ap-southeast-1, Singapore)
- Target: ClickHouse Cloud (ap-south-1, Mumbai)
- Network: Cross-region Asia Pacific with SSL/TLS
- Configuration: Batch size 2000, polling interval 5s

**Initial Full Sync (303K rows):**
| Metric | Value | vs Local |
|--------|-------|----------|
| Duration | 10 seconds | - |
| Throughput | **30,251 rows/sec** | **4.8x slower** |

**CDC Performance:**
| Metric | Value |
|--------|-------|
| Polling interval | 5 seconds |
| Detection latency | ~8 seconds |
| Test batch | 3,000 rows synced successfully |

**Key Insights:**
- Only 4.8x slower than local despite cross-region latency (much better than 20-55x in micro-benchmarks)
- Larger batch sizes (2000 vs 500) effectively amortize network overhead
- Streaming architecture minimizes round trips across regions
- CDC polling reliable with 5s interval despite network latency

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

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

## Architecture

```mermaid
flowchart LR
    PG[(PostgreSQL)] -->|Stream| Pool1[Connection Pool]
    Pool1 -->|Rows| Chan[Channel Buffer]
    Chan -->|Batch| Workers[4 Workers]
    Workers -->|Parallel Insert| Pool2[Connection Pool]
    Pool2 --> CH[(ClickHouse)]
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

## Quick Start

### 1. Start Local Environment

```bash
docker-compose up -d
```

Services:
- PostgreSQL: `localhost:5432` (user: chugger, password: secret, db: chugdb)
- ClickHouse: `localhost:9000`
- Adminer: `localhost:8080`
- Tabix: `localhost:8124`

### 2. Create Test Data

Access Adminer at http://localhost:8080:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (username, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com');
```

### 3. Run Migration

```bash
chug ingest \
  --pg-url "postgres://chugger:secret@localhost:5432/chugdb" \
  --ch-url "http://localhost:9000" \
  --table "users" \
  --limit 0
```

### 4. Verify

Access Tabix at http://localhost:8124:

```sql
SELECT * FROM users;
```

## Configuration

Generate config file:

```bash
chug sample-config
```

`.chug.yaml`:

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

## Usage

### Commands

**Test connections:**
```bash
chug connect --pg-url <pg-url> --ch-url <ch-url>
```

**Ingest data:**
```bash
chug ingest \
  --pg-url "postgres://user:pass@host:port/db" \
  --ch-url "http://host:port" \
  --table "tablename" \
  --limit 0 \
  --batch-size 500
```

**With config file:**
```bash
chug ingest --config .chug.yaml
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

## Change Data Capture

Enable continuous sync with polling:

```bash
chug ingest \
  --table "events" \
  --poll \
  --poll-delta "updated_at" \
  --poll-interval 60
```

**How it works:**

1. Initial full table sync
2. Track last seen value of delta column
3. Poll for rows where `delta_column > last_value`
4. Auto-create B-tree index for fast queries
5. Stream incremental changes

**Requirements:**

- Delta column must be monotonically increasing (timestamp, serial, etc.)
- For UPDATE detection, add trigger:

```sql
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_trigger
BEFORE UPDATE ON tablename
FOR EACH ROW EXECUTE FUNCTION update_timestamp();
```

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

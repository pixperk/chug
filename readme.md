#  CHUG: Blazing-Fast ETL Pipeline for PostgreSQL to ClickHouse

[![Go Report Card](https://goreportcard.com/badge/github.com/pixperk/chug)](https://goreportcard.com/report/github.com/pixperk/chug)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pixperk/chug)](https://github.com/pixperk/chug)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

Chug is a high-performance ETL (Extract, Transform, Load) tool designed to stream data from PostgreSQL databases to ClickHouse for analytics at ludicrous speed.

```
┌────────────────────────┐                              ┌────────────────────────┐
│                        │                              │                        │
│      PostgreSQL        │                              │      ClickHouse        │
│                        │                              │                        │
│    ┌──────────────┐    │                              │    ┌──────────────┐    │
│    │              │    │                              │    │              │    │
│    │    Tables    │    │                              │    │    Tables    │    │
│    │              │    │                              │    │              │    │
│    └──────────────┘    │                              │    └──────────────┘    │
│           │            │                              │           ▲            │
└───────────┼────────────┘                              └───────────┼────────────┘
            │                                                       │
            │                                                       │
            │                ┌────────────────────┐                 │
            │                │                    │                 │
            └───────────────▶│       CHUG        │─────────────────┘
                             │                    │
                             │  ┌─────┐ ┌──────┐  │
                             │  │  E  │ │  T   │  │
                             │  └──┬──┘ └──┬───┘  │
                             │     │      │       │
                             │     └──────┘       │
                             │         │          │
                             │      ┌──┴───┐      │
                             │      │  L   │      │
                             │      └──────┘      │
                             │                    │
                             └────────────────────┘
```

## 🌟 Features

- **Lightning Fast**: Optimized data transfer with batching and parallel processing
- **Secure**: Built-in protection against SQL injection attacks with proper parameterization and validation
- **Smart Schema Mapping**: Automatic schema conversion from PostgreSQL to ClickHouse with type inference
- **Resilient**: Retry mechanism with exponential backoff and jitter for handling transient failures
- **Observable**: Structured logging with Zap for comprehensive error reporting and progress tracking
- **Simple CLI**: Easy-to-use command line interface powered by Cobra for quick setup and operation
- **Resource Efficient**: Streaming data extraction and loading minimizes memory usage
- **SQL Compatibility**: Support for both PostgreSQL and ClickHouse SQL dialects
- **Containerized**: Ready-to-use Docker Compose setup for local development and testing

## 🛠️ Installation

### Prerequisites

- Go 1.20 or higher
- Docker and Docker Compose (for local development)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/pixperk/chug.git
cd chug

# Build the binary
go build -o chug

# Make it available in your PATH
mv chug /usr/local/bin/
```

## 🚀 Quick Start

### 1. Start Local Development Environment

Chug comes with a Docker Compose setup that includes PostgreSQL and ClickHouse:

```bash
docker-compose up -d
```

This will start:
- PostgreSQL on port 5432
- ClickHouse on ports 8123 (HTTP) and 9000 (TCP)
- Adminer (PostgreSQL UI) on port 8080
- Tabix (ClickHouse UI) on port 8124

### 2. Test Connections

Verify that both databases are accessible:

```bash
./chug connect --pg-url "postgres://chugger:secret@localhost:5432/chugdb" \
               --ch-url "localhost:9000"
```

### 3. Ingest Data

Transfer data from PostgreSQL to ClickHouse:

```bash
./chug ingest --pg-url "postgres://chugger:secret@localhost:5432/chugdb" \
              --ch-url "localhost:9000" \
              --table "users" \
              --limit 10000 \
              --batch-size 1000
```

## 🧰 Command Reference

### `connect`

Test connectivity to PostgreSQL and ClickHouse:

```bash
./chug connect --pg-url <postgres-connection-string> --ch-url <clickhouse-connection-string>
```

### `ingest`

Transfer data from PostgreSQL to ClickHouse:

```bash
./chug ingest --pg-url <postgres-connection-string> \
              --ch-url <clickhouse-connection-string> \
              --table <table-name> \
              [--limit <max-rows>] \
              [--batch-size <rows-per-batch>]
```

Options:
- `--pg-url`: PostgreSQL connection string (required)
- `--ch-url`: ClickHouse connection string (required)
- `--table`: Name of the table to transfer (required)
- `--limit`: Maximum number of rows to extract (default: 1000)
- `--batch-size`: Number of rows per INSERT batch (default: 500)

## 🔒 Security Features

Chug includes several security measures:

1. **SQL Injection Prevention**:
   - Table and column names are properly quoted and validated
   - Parameterized queries are used for data extraction
   - Input validation for identifiers

2. **Error Handling**:
   - Detailed error messages with proper context
   - Secure error reporting that doesn't expose sensitive information

## 🏗️ Architecture

Chug follows a clean, modular architecture:

### ETL Pipeline

1. **Extract**: Data is extracted from PostgreSQL with optional row limits
2. **Transform**: 
   - Table schemas are converted to ClickHouse-compatible formats
   - Data types are mapped appropriately
3. **Load**:
   - Target tables are created in ClickHouse
   - Data is loaded in configurable batch sizes
   - Retry logic handles transient failures

### Components

- **cmd**: Command-line interface definitions using Cobra
- **internal/db**: Database connection utilities
- **internal/etl**: Core ETL functionality
  - Extraction logic
  - Schema mapping
  - Data loading
  - Helper utilities
- **internal/logx**: Structured logging with Zap

## 📅 Roadmap

The following features are planned for future releases:

- **YAML Configuration**: Replace CLI flags with structured YAML configuration for more complex setups
- **Export Capabilities**: Support for exporting data to CSV and Parquet formats
- **CDC Polling**: Light-weight Change Data Capture for incremental data ingestion
- **Prometheus Metrics**: Performance monitoring and alerting integration
- **Schema Evolution**: Automatic handling of schema changes
- **Parallel Extraction**: Multi-threaded data extraction for improved performance

## 🔄 Development Workflow

1. **Setup local environment**:
   ```bash
   docker-compose up -d
   ```

2. **Create test data in PostgreSQL**:
   Connect to http://localhost:8080 with:
   - System: PostgreSQL
   - Server: postgres
   - Username: chugger
   - Password: secret
   - Database: chugdb

3. **Run data transfer**:
   ```bash
   go run main.go ingest --pg-url "postgres://chugger:secret@localhost:5432/chugdb" \
                        --ch-url "localhost:9000" \
                        --table "your_table"
   ```

4. **Verify data in ClickHouse**:
   Access Tabix at http://localhost:8124 and connect to the ClickHouse server.

## 💡 Usage Examples

### Migrating a User Analytics Table

```bash
# First test connectivity
./chug connect --pg-url "postgres://analytics_user:pass@pg-server:5432/analytics" \
              --ch-url "clickhouse-server:9000"

# Transfer user_events table with a large batch size for performance
./chug ingest --pg-url "postgres://analytics_user:pass@pg-server:5432/analytics" \
             --ch-url "clickhouse-server:9000" \
             --table "user_events" \
             --batch-size 5000
```

### Partial Data Migration with Limit

```bash
# Transfer only the first 1 million rows from a large table
./chug ingest --pg-url "postgres://app:password@postgres.example.com/app_db" \
             --ch-url "clickhouse.example.com:9000" \
             --table "transactions" \
             --limit 1000000 \
             --batch-size 10000
```

### Production Environment Setup

For production environments, it's recommended to:

1. Use a dedicated service account with read-only access to source PostgreSQL tables
2. Set up appropriate network security between services
3. Consider running Chug in a container:

```bash
docker run -d --name chug-etl \
  pixperk/chug ingest \
  --pg-url "postgres://readonly:password@pg-prod.internal:5432/app" \
  --ch-url "clickhouse-analytics.internal:9000" \
  --table "customers" \
  --batch-size 1000
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/pixperk">Pixperk</a></sub>
</div>

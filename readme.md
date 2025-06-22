#  CHUG: Blazing-Fast ETL Pipeline for PostgreSQL to ClickHouse

[![Go Report Card](https://goreportcard.com/badge/github.com/pixperk/chug)](https://goreportcard.com/report/github.com/pixperk/chug)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pixperk/chug)](https://github.com/pixperk/chug)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

Chug is a high-performance ETL (Extract, Transform, Load) tool designed to stream data from PostgreSQL databases to ClickHouse for analytics at ludicrous speed.

![Chug Architecture](assets/chug_arch.png)

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
- **YAML Configuration**: Support for structured YAML configuration for more complex setups
- **CDC Polling**: Light-weight Change Data Capture for incremental data ingestion through polling
- **UUID Support**: Enhanced handling of UUID data types for seamless migration
- **CSV Export**: Ability to export data from ClickHouse tables to CSV files

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
              [--batch-size <rows-per-batch>] \
              [--config <path-to-config-file>] \
              [--poll] \
              [--poll-delta <delta-column>] \
              [--poll-interval <seconds>]
```

Options:
- `--pg-url`: PostgreSQL connection string (required if not in config)
- `--ch-url`: ClickHouse connection string (required if not in config)
- `--table`: Name of the table to transfer (required if not in config)
- `--limit`: Maximum number of rows to extract (default: 1000)
- `--batch-size`: Number of rows per INSERT batch (default: 500)
- `--config`: Path to YAML configuration file (default: .chug.yaml)
- `--poll`: Enable continuous polling for changes after initial ingest
- `--poll-delta`: Column name to track changes, usually a timestamp or incrementing ID (default: updated_at)
- `--poll-interval`: Polling interval in seconds (default: 30)

### `sample-config`

Generate a sample YAML configuration file:

```bash
./chug sample-config
```

This will create a `.chug.yaml` file in the current directory with default settings.

### `export`

Export data from ClickHouse to CSV:

```bash
./chug export --ch-url <clickhouse-connection-string> \
             --table <table-name> \
             --format csv \
             --out <output-directory>
```

Options:
- `--ch-url`: ClickHouse connection string (required if not in config)
- `--table`: Name of the table to export (required if not in config)
- `--format`: Export format (currently only csv is supported)
- `--out`: Output directory for exported files (default: current directory)
- `--config`: Path to YAML configuration file (default: .chug.yaml)

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
  - **connect.go**: Connection testing functionality
  - **ingest.go**: Data ingestion commands
  - **poll.go**: Polling functionality for CDC
  - **sample_config.go**: Configuration sample generator
  - **root.go**: Root command and global flags
- **internal/db**: Database connection utilities
  - **clickhouse.go**: ClickHouse connection handling
  - **postgres.go**: PostgreSQL connection handling
- **internal/etl**: Core ETL functionality
  - **extractor.go**: Data extraction from PostgreSQL
  - **transform.go**: Schema and data transformation
  - **load.go**: Data loading into ClickHouse
  - **pgToCh.go**: PostgreSQL to ClickHouse type mapping
  - **retry.go**: Retry mechanisms for resilient operations
- **internal/config**: Configuration handling
  - **config.go**: YAML configuration loading and parsing
- **internal/poller**: Change data capture functionality
  - **poller.go**: Implementation of delta-based polling
- **internal/logx**: Structured logging with Zap

## 📅 Roadmap

The following features are planned for future releases:

- **Parquet Export**: Support for exporting data to Parquet format
- **Prometheus Metrics**: Performance monitoring and alerting integration
- **Conflict Resolution**: Strategies for handling conflicting updates during synchronization
- **Data Validation**: Options for validating data integrity during and after transfer

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

### Using YAML Configuration

For more complex setups, use a YAML configuration file:

1. Generate a sample configuration:
   ```bash
   ./chug sample-config
   ```

2. Edit the generated `.chug.yaml` file:
   ```yaml
   # PostgreSQL connection URL
   pg_url: "postgres://analytics_user:pass@pg-server:5432/analytics"
   
   # ClickHouse HTTP interface URL
   ch_url: "clickhouse-server:9000"
   
   # Table to ingest from Postgres
   table: "user_events"
   
   # Max rows to fetch
   limit: 10000
   
   # Batch size per insert
   batch_size: 2000
   
   # Polling configuration
   polling:
     # Enable polling for changes after initial ingest
     enabled: true
     # Column name to track changes (usually a timestamp)
     delta_column: "updated_at"
     # Polling interval in seconds
     interval_seconds: 60
   ```

3. Run Chug with the configuration file:
   ```bash
   ./chug ingest --config .chug.yaml
   ```

### Continuous Data Synchronization with Polling

To keep your ClickHouse tables in sync with PostgreSQL:

```bash
# Use polling to continuously monitor for changes
./chug ingest --pg-url "postgres://app:password@postgres.example.com/app_db" \
             --ch-url "clickhouse.example.com:9000" \
             --table "transactions" \
             --poll \
             --poll-delta "modified_timestamp" \
             --poll-interval 300
```

This will:
1. Perform an initial full table import
2. Monitor the `modified_timestamp` column for new or updated records
3. Check for changes every 5 minutes (300 seconds)
4. Automatically import only the changed records

### Exporting Data to CSV

To export data from ClickHouse to a CSV file:

```bash
# Export a table to CSV
./chug export --ch-url "clickhouse.example.com:9000" \
             --table "user_analytics" \
             --format csv \
             --out "/path/to/exports"
```

This will create a file named `user_analytics.csv` in the specified output directory containing all data from the table.

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

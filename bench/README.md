# CHUG Performance Benchmarking

Comprehensive benchmarking with percentile metrics (p50, p95, p99, p999) for realistic performance analysis.

## Quick Start

### 1. Setup

```bash
# Start local databases
docker-compose up -d

# Create benchmark tables (100K rows)
make bench-setup-local    # For local Docker
make bench-setup-remote   # For cloud databases (requires .env config)
```

### 2. Run Benchmarks

**Local (Docker - fast but unrealistic):**
```bash
make bench-local
```

**Remote (Cloud - production-like, requires .env):**
```bash
# Configure .env with REMOTE_PG and REMOTE_CLICKHOUSE
make bench-remote
```

**Specific benchmarks:**
```bash
make bench-extract    # Extraction only
make bench-insert     # Insertion only
make bench-cdc        # CDC only
make bench-multi      # Multi-table only
```

## Available Commands

```bash
make help                # Show all commands
make bench-setup-local   # Create test tables (local)
make bench-setup-remote  # Create test tables (remote)
make bench-local         # All benchmarks (local)
make bench-remote        # All benchmarks (remote)
make bench-both          # Compare local vs remote
make bench-extract       # Extraction benchmarks only
make bench-insert        # Insertion benchmarks only
make bench-cdc           # CDC benchmarks only
make clean               # Clean up benchmark artifacts
```

## Understanding Percentile Metrics

### Why Percentiles Matter

**Average hides outliers.** Percentiles show the real distribution:

- **p50 (median)**: 50% of requests are faster
- **p95**: 95% of requests are faster (what most users experience)
- **p99**: 99% of requests are faster
- **p999**: 99.9% of requests are faster (tail latency)

### Example Output

```
Extract_bench_data_limit_10000          iterations=20
  min=45ms p50=52ms p95=68ms p99=89ms p999=95ms max=98ms mean=55ms±12ms
  throughput: 192.31 ops/sec
```

**What this means:**
- Half of extractions complete in ≤52ms (p50)
- 95% complete in ≤68ms (p95)
- Worst case was 98ms
- Some variance (±12ms standard deviation)

### Local vs Remote Comparison

**Local Docker (unrealistic):**
```
Extract_10K_rows:  p50=8ms  p95=12ms  p99=15ms
Insert_10K_rows:   p50=15ms p95=22ms  p99=28ms
```

**Remote with 50ms latency (realistic):**
```
Extract_10K_rows:  p50=156ms p95=203ms p99=245ms
Insert_10K_rows:   p50=312ms p95=401ms p99=489ms
```

Network latency dominates remote performance (20x slower)!

## Testing Remote Databases

### Cloud Databases

Configure your cloud database URLs in `.env`:

```bash
# .env file
REMOTE_PG=postgres://user:pass@rds.amazonaws.com:5432/db
REMOTE_CLICKHOUSE=https://default:password@clickhouse.cloud:9440

# For ClickHouse Cloud, use HTTPS with port 9440 (native protocol)
# For self-hosted ClickHouse, use http://host:9000
```

Then run:
```bash
make bench-remote
```

### Simulate Network Latency

Test performance under different network conditions:

```bash
# Add 50ms latency (Linux)
sudo tc qdisc add dev eth0 root netem delay 50ms

# Run benchmarks
make bench-local

# Remove latency
sudo tc qdisc del dev eth0 root
```

### Measure Actual Latency

```bash
# PostgreSQL ping
time psql -h your-db.com -U user -d db -c "SELECT 1"

# ClickHouse ping
time curl http://your-clickhouse.com:8123/ping
```

## Benchmark Types

### 1. Extraction (PostgreSQL → Memory)

Tests streaming data extraction:
- 1K rows
- 10K rows
- 100K rows

**Measures:** Network latency, connection pool efficiency, streaming performance

### 2. Insertion (Memory → ClickHouse)

Tests parallel batch insertion with 4 workers:
- Various row counts (1K, 10K, 50K)
- Different batch sizes (500, 1000, 2000)

**Analyzes:** Batch size impact, worker pool efficiency, network round-trips

### 3. CDC (Delta Column Filtering)

Tests incremental change capture:
- Indexed query performance
- Delta filtering efficiency

**Validates:** Polling overhead, index effectiveness

### 4. Multi-Table (Parallel Ingestion)

Tests concurrent table processing:
- 3 tables in parallel
- Connection pool sharing

**Demonstrates:** Parallelism benefits, aggregate throughput

## Advanced Usage

### Custom Iterations

```bash
make bench-local BENCH_ITERATIONS=50
```

### Different Table

```bash
make bench-extract BENCH_TABLE=my_production_table
```

### Save Results

```bash
make bench-all    # Automatically saves to bench_results/
```

### Regression Testing

```bash
# Create baseline
make bench-regression

# After code changes
make bench-regression   # Compares against baseline
```

## Performance Expectations

### Local Docker (Baseline)

| Benchmark | p50 | p95 | Throughput |
|-----------|-----|-----|------------|
| Extract 10K | 8ms | 12ms | 125 ops/s |
| Insert 10K (batch 500) | 15ms | 22ms | 67 ops/s |
| Multi-table (3x10K) | 25ms | 35ms | 120 ops/s |

### Remote (15ms latency)

| Benchmark | p50 | p95 | Throughput |
|-----------|-----|-----|------------|
| Extract 10K | 156ms | 203ms | 6.4 ops/s |
| Insert 10K (batch 500) | 312ms | 401ms | 3.2 ops/s |
| Multi-table (3x10K) | 380ms | 490ms | 7.9 ops/s |

### Remote (50ms latency)

| Benchmark | p50 | p95 | Throughput |
|-----------|-----|-----|------------|
| Extract 10K | 450ms | 580ms | 2.2 ops/s |
| Insert 10K (batch 500) | 900ms | 1150ms | 1.1 ops/s |
| Multi-table (3x10K) | 1100ms | 1400ms | 2.7 ops/s |

## Interpreting Results

### Good Performance

```
p50=50ms p95=75ms p99=95ms
```

✓ Low latency
✓ Narrow distribution (p95/p50 < 2x)
✓ Consistent performance

### Performance Issues

```
p50=200ms p95=800ms p99=2000ms
```

✗ High latency
✗ Wide distribution (p95/p50 = 4x)
✗ Severe tail latency

**Causes:**
- High network latency
- Database overload
- Insufficient connection pooling
- Resource contention

## Optimization Guide

**High tail latency (p99 >> p95):**
- Check for GC pauses
- Database lock contention
- Network jitter

**Low throughput:**
- Increase batch size
- Verify connection pool settings
- Check CPU/memory availability

**Slow extraction:**
- Add database indexes
- Optimize query plan
- Verify connection pooling

**Slow insertion:**
- Increase batch size for high latency
- Check ClickHouse disk I/O
- Reduce network round-trips

## Best Practices

1. **Run enough iterations** - Minimum 20, prefer 50+ for production
2. **Warm up connections** - First runs establish pools
3. **Isolate load** - No concurrent operations
4. **Test realistic scenarios** - Production-like data and network
5. **Document environment** - CPU, RAM, network latency, DB versions

## Example Report

```
Environment:
  CPU: AMD Ryzen 9 5900X (12 cores)
  RAM: 32GB
  Network: 15ms to databases
  PostgreSQL: 16.0 (AWS RDS)
  ClickHouse: 23.10 (Cloud)

Results (50 iterations):
  Extract_10K:   p50=180ms p95=245ms p99=312ms
  Insert_10K:    p50=320ms p95=425ms p99=518ms
  MultiTable_3x: p50=380ms p95=490ms p99=592ms

Network latency: ~15ms (confirmed)
Batch size 500 optimal for this latency
Parallel processing shows 2.1x throughput
p99 within 2x of p50 (good consistency)
```

## Troubleshooting

**Connection refused:**
- Check databases are running
- Verify firewall rules
- Test with psql/curl first

**Inconsistent results:**
- Run more iterations
- Isolate from other load
- Check background processes

**Benchmark hangs:**
- Database locked/overloaded
- Network timeout
- Check logs and connections

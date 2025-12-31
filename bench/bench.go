package bench

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/etl"
)

type BenchmarkResult struct {
	Name       string
	Iterations int
	Durations  []time.Duration
	BytesAlloc uint64
	Allocs     uint64
}

type PercentileStats struct {
	Min    time.Duration
	P50    time.Duration
	P95    time.Duration
	P99    time.Duration
	P999   time.Duration
	Max    time.Duration
	Mean   time.Duration
	StdDev time.Duration
}

func (r *BenchmarkResult) Percentiles() PercentileStats {
	if len(r.Durations) == 0 {
		return PercentileStats{}
	}

	sorted := make([]time.Duration, len(r.Durations))
	copy(sorted, r.Durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	percentile := func(p float64) time.Duration {
		idx := int(float64(len(sorted)) * p)
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(len(sorted))

	var variance float64
	for _, d := range sorted {
		diff := float64(d - mean)
		variance += diff * diff
	}
	stddev := time.Duration(math.Sqrt(variance / float64(len(sorted))))

	return PercentileStats{
		Min:    sorted[0],
		P50:    percentile(0.50),
		P95:    percentile(0.95),
		P99:    percentile(0.99),
		P999:   percentile(0.999),
		Max:    sorted[len(sorted)-1],
		Mean:   mean,
		StdDev: stddev,
	}
}

func (s PercentileStats) String() string {
	return fmt.Sprintf(
		"min=%v p50=%v p95=%v p99=%v p999=%v max=%v mean=%v±%v",
		s.Min, s.P50, s.P95, s.P99, s.P999, s.Max, s.Mean, s.StdDev,
	)
}

type Benchmarker struct {
	PgPool     *pgxpool.Pool
	ChURL      string
	Ctx        context.Context
	Warmup     int
	Iterations int
}

func NewBenchmarker(ctx context.Context, pgURL, chURL string, warmup, iterations int) (*Benchmarker, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &Benchmarker{
		PgPool:     pool,
		ChURL:      chURL,
		Ctx:        ctx,
		Warmup:     warmup,
		Iterations: iterations,
	}, nil
}

func (b *Benchmarker) Close() {
	if b.PgPool != nil {
		b.PgPool.Close()
	}
}

func (b *Benchmarker) BenchmarkExtraction(tableName string, limit int) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:       fmt.Sprintf("Extract_%s_limit_%d", tableName, limit),
		Iterations: b.Iterations,
		Durations:  make([]time.Duration, 0, b.Iterations),
	}

	// Warmup
	for i := 0; i < b.Warmup; i++ {
		stream, _ := etl.ExtractTableDataStreaming(b.Ctx, b.PgPool, tableName, &limit)
		for range stream.RowChan {
		}
	}

	// Actual benchmark
	for i := 0; i < b.Iterations; i++ {
		start := time.Now()

		stream, err := etl.ExtractTableDataStreaming(b.Ctx, b.PgPool, tableName, &limit)
		if err != nil {
			continue
		}

		rowCount := 0
		for range stream.RowChan {
			rowCount++
		}

		duration := time.Since(start)
		result.Durations = append(result.Durations, duration)
	}

	return result
}

func (b *Benchmarker) BenchmarkInsertion(tableName string, rowCount, batchSize int) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:       fmt.Sprintf("Insert_%s_%d_rows_batch_%d", tableName, rowCount, batchSize),
		Iterations: b.Iterations,
		Durations:  make([]time.Duration, 0, b.Iterations),
	}

	// Create test table
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id Int32,
		name String,
		value Float64,
		created_at DateTime
	) ENGINE = MergeTree() ORDER BY id`, tableName)
	etl.CreateTable(b.ChURL, ddl)

	columns := []string{"id", "name", "value", "created_at"}

	// Warmup
	for i := 0; i < b.Warmup; i++ {
		rowChan := make(chan []any, rowCount)
		for j := 0; j < rowCount; j++ {
			rowChan <- []any{j, "test", 123.45, "2024-01-01 00:00:00"}
		}
		close(rowChan)
		etl.InsertRowsStreaming(b.Ctx, b.ChURL, tableName, columns, rowChan, batchSize)
	}

	// Actual benchmark
	for i := 0; i < b.Iterations; i++ {
		rowChan := make(chan []any, rowCount) // Buffered channel to hold all rows

		// Pre-fill the channel before timing
		for j := 0; j < rowCount; j++ {
			rowChan <- []any{j, "test", 123.45, "2024-01-01 00:00:00"}
		}
		close(rowChan)

		start := time.Now()
		err := etl.InsertRowsStreaming(b.Ctx, b.ChURL, tableName, columns, rowChan, batchSize)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("Insertion error (iteration %d): %v\n", i, err)
			continue
		}

		result.Durations = append(result.Durations, duration)
	}

	// Cleanup
	etl.CreateTable(b.ChURL, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	return result
}

func (b *Benchmarker) BenchmarkMultiTable(tables []string, rowsPerTable, batchSize int) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:       fmt.Sprintf("MultiTable_%d_tables_%d_rows_batch_%d", len(tables), rowsPerTable, batchSize),
		Iterations: b.Iterations,
		Durations:  make([]time.Duration, 0, b.Iterations),
	}

	cfg := &config.Config{
		PostgresURL:   b.PgPool.Config().ConnString(),
		ClickHouseURL: b.ChURL,
		BatchSize:     batchSize,
	}

	var tableConfigs []config.TableConfig
	for _, table := range tables {
		tableConfigs = append(tableConfigs, config.TableConfig{Name: table})
	}
	cfg.Tables = tableConfigs

	// Warmup
	for i := 0; i < b.Warmup; i++ {
		// Simulate multi-table ingestion
	}

	// Actual benchmark
	for i := 0; i < b.Iterations; i++ {
		start := time.Now()

		// Simulate parallel table processing
		// This would call ingestMultipleTables in real scenario

		duration := time.Since(start)
		result.Durations = append(result.Durations, duration)
	}

	return result
}

func (b *Benchmarker) BenchmarkCDC(tableName, deltaCol string, limit int) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:       fmt.Sprintf("CDC_%s_delta_%s", tableName, deltaCol),
		Iterations: b.Iterations,
		Durations:  make([]time.Duration, 0, b.Iterations),
	}

	lastSeen := "2024-01-01 00:00:00"

	// Warmup
	for i := 0; i < b.Warmup; i++ {
		stream, _ := etl.ExtractTableDataSince(b.Ctx, b.PgPool, tableName, deltaCol, lastSeen, &limit)
		for range stream.Rows {
		}
	}

	// Actual benchmark
	for i := 0; i < b.Iterations; i++ {
		start := time.Now()

		stream, err := etl.ExtractTableDataSince(b.Ctx, b.PgPool, tableName, deltaCol, lastSeen, &limit)
		if err != nil {
			continue
		}

		for range stream.Rows {
		}

		duration := time.Since(start)
		result.Durations = append(result.Durations, duration)
	}

	return result
}

func PrintResults(results []*BenchmarkResult) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 80) + "\n")

	for _, r := range results {
		stats := r.Percentiles()
		fmt.Printf("%-60s iterations=%d\n", r.Name, r.Iterations)
		fmt.Printf("  %s\n", stats.String())
		fmt.Printf("  throughput: %.2f ops/sec\n\n", 1.0/stats.Mean.Seconds())
	}
}

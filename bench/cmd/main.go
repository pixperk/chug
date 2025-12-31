package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/pixperk/chug/bench"
	"github.com/pixperk/chug/internal/logx"
)

func main() {
	// Initialize logger for benchmarks
	logx.InitLogger()

	mode := flag.String("mode", "local", "Benchmark mode: local, remote, both")
	table := flag.String("table", "bench_data", "Table name for benchmarks")
	deltaCol := flag.String("delta-col", "updated_at", "Delta column for CDC benchmarks")
	warmup := flag.Int("warmup", 3, "Number of warmup iterations")
	iterations := flag.Int("iterations", 20, "Number of benchmark iterations")
	benchType := flag.String("bench", "all", "Benchmark type: extract, insert, cdc, multi, all")

	flag.Parse()

	// Try to load .env from current directory or parent directories
	if err := godotenv.Load(".env"); err != nil {
		if err := godotenv.Load("../.env"); err != nil {
			if err := godotenv.Load("../../.env"); err != nil {
				log.Printf("Warning: .env file not found, using defaults")
			}
		}
	}

	ctx := context.Background()

	switch *mode {
	case "local":
		runBenchmark(ctx, "LOCAL", os.Getenv("LOCAL_PG"), os.Getenv("LOCAL_CLICKHOUSE"),
			*table, *deltaCol, *warmup, *iterations, *benchType)

	case "remote":
		runBenchmark(ctx, "REMOTE", os.Getenv("REMOTE_PG"), os.Getenv("REMOTE_CLICKHOUSE"),
			*table, *deltaCol, *warmup, *iterations, *benchType)

	case "both":
		fmt.Println(strings.Repeat("=", 80))
		fmt.Println("LOCAL BENCHMARKS")
		fmt.Println(strings.Repeat("=", 80))
		runBenchmark(ctx, "LOCAL", os.Getenv("LOCAL_PG"), os.Getenv("LOCAL_CLICKHOUSE"),
			*table, *deltaCol, *warmup, *iterations, *benchType)

		fmt.Println("\n" + strings.Repeat("=", 80))
		fmt.Println("REMOTE BENCHMARKS")
		fmt.Println(strings.Repeat("=", 80))
		runBenchmark(ctx, "REMOTE", os.Getenv("REMOTE_PG"), os.Getenv("REMOTE_CLICKHOUSE"),
			*table, *deltaCol, *warmup, *iterations, *benchType)

	default:
		log.Fatalf("Unknown mode: %s (use: local, remote, both)", *mode)
	}
}

func runBenchmark(ctx context.Context, envName, pgURL, chURL, table, deltaCol string,
	warmup, iterations int, benchType string) {

	if pgURL == "" || chURL == "" {
		log.Fatalf("%s database URLs not set in .env file", envName)
	}

	fmt.Printf("\n%s Environment\n", envName)
	fmt.Printf("PostgreSQL: %s\n", maskPassword(pgURL))
	fmt.Printf("ClickHouse: %s\n\n", maskPassword(chURL))

	benchmarker, err := bench.NewBenchmarker(ctx, pgURL, chURL, warmup, iterations)
	if err != nil {
		log.Fatalf("Failed to create benchmarker: %v", err)
	}
	defer benchmarker.Close()

	var results []*bench.BenchmarkResult

	switch benchType {
	case "extract":
		fmt.Println("Running EXTRACTION benchmarks...")
		results = runExtractionBenchmarks(benchmarker, table)

	case "insert":
		fmt.Println("Running INSERTION benchmarks...")
		results = runInsertionBenchmarks(benchmarker, table)

	case "cdc":
		fmt.Println("Running CDC benchmarks...")
		results = runCDCBenchmarks(benchmarker, table, deltaCol)

	case "multi":
		fmt.Println("Running MULTI-TABLE benchmarks...")
		results = runMultiTableBenchmarks(benchmarker)

	case "all":
		fmt.Println("Running ALL benchmarks...")
		results = append(results, runExtractionBenchmarks(benchmarker, table)...)
		results = append(results, runInsertionBenchmarks(benchmarker, table)...)
		results = append(results, runCDCBenchmarks(benchmarker, table, deltaCol)...)
		results = append(results, runMultiTableBenchmarks(benchmarker)...)

	default:
		log.Fatalf("Unknown benchmark type: %s (use: extract, insert, cdc, multi, all)", benchType)
	}

	bench.PrintResults(results)
	printOptimizationTips(results)
}

func runExtractionBenchmarks(b *bench.Benchmarker, table string) []*bench.BenchmarkResult {
	return []*bench.BenchmarkResult{
		b.BenchmarkExtraction(table, 1000),
		b.BenchmarkExtraction(table, 10000),
		b.BenchmarkExtraction(table, 100000),
	}
}

func runInsertionBenchmarks(b *bench.Benchmarker, table string) []*bench.BenchmarkResult {
	return []*bench.BenchmarkResult{
		b.BenchmarkInsertion(table+"_insert_test", 1000, 500),
		b.BenchmarkInsertion(table+"_insert_test", 10000, 500),
		b.BenchmarkInsertion(table+"_insert_test", 10000, 1000),
		b.BenchmarkInsertion(table+"_insert_test", 50000, 2000),
	}
}

func runCDCBenchmarks(b *bench.Benchmarker, table, deltaCol string) []*bench.BenchmarkResult {
	return []*bench.BenchmarkResult{
		b.BenchmarkCDC(table, deltaCol, 1000),
		b.BenchmarkCDC(table, deltaCol, 10000),
	}
}

func runMultiTableBenchmarks(b *bench.Benchmarker) []*bench.BenchmarkResult {
	tables := []string{"bench_table_1", "bench_table_2", "bench_table_3"}
	return []*bench.BenchmarkResult{
		b.BenchmarkMultiTable(tables, 10000, 500),
	}
}

func printOptimizationTips(results []*bench.BenchmarkResult) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("OPTIMIZATION TIPS")
	fmt.Println(strings.Repeat("=", 80))

	for _, r := range results {
		stats := r.Percentiles()

		if stats.P99 > stats.P95*2 {
			fmt.Printf("\n⚠ High tail latency in %s:\n", r.Name)
			fmt.Printf("  p99/p95 ratio: %.2fx\n", float64(stats.P99)/float64(stats.P95))
			fmt.Println("  Suggestions:")
			fmt.Println("  - Check for GC pauses")
			fmt.Println("  - Database lock contention")
			fmt.Println("  - Network jitter")
		}

		throughput := 1.0 / stats.Mean.Seconds()
		if strings.Contains(r.Name, "Insert") && throughput < 10 {
			fmt.Printf("\n⚠ Low insertion throughput in %s:\n", r.Name)
			fmt.Printf("  Throughput: %.2f ops/sec\n", throughput)
			fmt.Println("  Suggestions:")
			fmt.Println("  - Increase batch size")
			fmt.Println("  - Check network latency")
			fmt.Println("  - Verify ClickHouse disk I/O")
		}

		if strings.Contains(r.Name, "Extract") && stats.P50.Seconds() > 0.5 {
			fmt.Printf("\n⚠ Slow extraction in %s:\n", r.Name)
			fmt.Printf("  p50 latency: %v\n", stats.P50)
			fmt.Println("  Suggestions:")
			fmt.Println("  - Add database indexes")
			fmt.Println("  - Optimize query plan")
			fmt.Println("  - Check connection pool settings")
		}
	}

	fmt.Println()
}

func maskPassword(url string) string {
	if strings.Contains(url, "@") {
		parts := strings.Split(url, "@")
		if len(parts) == 2 {
			userInfo := strings.Split(parts[0], "://")
			if len(userInfo) == 2 {
				user := strings.Split(userInfo[1], ":")
				if len(user) == 2 {
					return userInfo[0] + "://" + user[0] + ":***@" + parts[1]
				}
			}
		}
	}
	return url
}

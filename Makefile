.PHONY: help bench bench-local bench-remote bench-both bench-all bench-extract bench-insert bench-cdc bench-multi bench-setup clean

BENCH_TABLE ?= bench_data
BENCH_ITERATIONS ?= 20

# Load environment variables from .env
include .env
export $(shell sed 's/=.*//' .env)

help:
	@echo "CHUG Benchmark Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make bench-setup-local    - Create benchmark tables in LOCAL database"
	@echo "  make bench-setup-remote   - Create benchmark tables in REMOTE database"
	@echo ""
	@echo "Benchmarks:"
	@echo "  make bench-local          - Run all benchmarks against local Docker"
	@echo "  make bench-remote         - Run all benchmarks against remote cloud"
	@echo "  make bench-both           - Run benchmarks on both local and remote"
	@echo ""
	@echo "Specific benchmarks:"
	@echo "  make bench-extract        - Run extraction benchmarks (local)"
	@echo "  make bench-insert         - Run insertion benchmarks (local)"
	@echo "  make bench-cdc            - Run CDC benchmarks (local)"
	@echo ""
	@echo "Advanced:"
	@echo "  make bench-comparison     - Compare local vs remote performance"
	@echo ""
	@echo "Configuration:"
	@echo "  BENCH_TABLE         Table name (default: bench_data)"
	@echo "  BENCH_ITERATIONS    Number of iterations (default: 20)"
	@echo "  Uses LOCAL_PG, LOCAL_CLICKHOUSE, REMOTE_PG, REMOTE_CLICKHOUSE from .env"

bench-setup-local:
	@echo "Setting up benchmark tables in LOCAL database..."
	@psql $(LOCAL_PG) -c "DROP TABLE IF EXISTS bench_data CASCADE;"
	@psql $(LOCAL_PG) -c "CREATE TABLE bench_data (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INTEGER, score FLOAT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
	@psql $(LOCAL_PG) -c "INSERT INTO bench_data (name, email, age, score) SELECT 'User_' || i, 'user' || i || '@example.com', 20 + (i % 50), random() * 100 FROM generate_series(1, 100000) AS i;"
	@psql $(LOCAL_PG) -c "CREATE INDEX idx_bench_updated_at ON bench_data(updated_at);"
	@psql $(LOCAL_PG) -c "DROP TABLE IF EXISTS bench_table_1, bench_table_2, bench_table_3 CASCADE;"
	@psql $(LOCAL_PG) -c "CREATE TABLE bench_table_1 AS SELECT * FROM bench_data LIMIT 10000;"
	@psql $(LOCAL_PG) -c "CREATE TABLE bench_table_2 AS SELECT * FROM bench_data LIMIT 10000;"
	@psql $(LOCAL_PG) -c "CREATE TABLE bench_table_3 AS SELECT * FROM bench_data LIMIT 10000;"
	@psql $(LOCAL_PG) -c "CREATE INDEX idx_bench_table_1_updated ON bench_table_1(updated_at);"
	@psql $(LOCAL_PG) -c "CREATE INDEX idx_bench_table_2_updated ON bench_table_2(updated_at);"
	@psql $(LOCAL_PG) -c "CREATE INDEX idx_bench_table_3_updated ON bench_table_3(updated_at);"
	@echo "Local benchmark tables created successfully!"

bench-setup-remote:
	@echo "Setting up benchmark tables in REMOTE database..."
	@psql "$(REMOTE_PG)" -c "DROP TABLE IF EXISTS bench_data CASCADE;"
	@psql "$(REMOTE_PG)" -c "CREATE TABLE bench_data (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INTEGER, score FLOAT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
	@psql "$(REMOTE_PG)" -c "INSERT INTO bench_data (name, email, age, score) SELECT 'User_' || i, 'user' || i || '@example.com', 20 + (i % 50), random() * 100 FROM generate_series(1, 100000) AS i;"
	@psql "$(REMOTE_PG)" -c "CREATE INDEX idx_bench_updated_at ON bench_data(updated_at);"
	@psql "$(REMOTE_PG)" -c "DROP TABLE IF EXISTS bench_table_1, bench_table_2, bench_table_3 CASCADE;"
	@psql "$(REMOTE_PG)" -c "CREATE TABLE bench_table_1 AS SELECT * FROM bench_data LIMIT 10000;"
	@psql "$(REMOTE_PG)" -c "CREATE TABLE bench_table_2 AS SELECT * FROM bench_data LIMIT 10000;"
	@psql "$(REMOTE_PG)" -c "CREATE TABLE bench_table_3 AS SELECT * FROM bench_data LIMIT 10000;"
	@psql "$(REMOTE_PG)" -c "CREATE INDEX idx_bench_table_1_updated ON bench_table_1(updated_at);"
	@psql "$(REMOTE_PG)" -c "CREATE INDEX idx_bench_table_2_updated ON bench_table_2(updated_at);"
	@psql "$(REMOTE_PG)" -c "CREATE INDEX idx_bench_table_3_updated ON bench_table_3(updated_at);"
	@echo "Remote benchmark tables created successfully!"

bench-local:
	@echo "Running LOCAL benchmarks..."
	@./bench/cmd/bench -mode local -bench all -table $(BENCH_TABLE) -iterations $(BENCH_ITERATIONS)

bench-remote:
	@echo "Running REMOTE benchmarks..."
	@./bench/cmd/bench -mode remote -bench all -table $(BENCH_TABLE) -iterations $(BENCH_ITERATIONS)

bench-both:
	@echo "Running BOTH local and remote benchmarks..."
	@./bench/cmd/bench -mode both -bench all -table $(BENCH_TABLE) -iterations $(BENCH_ITERATIONS)

bench-extract:
	@echo "Running extraction benchmarks (local)..."
	@./bench/cmd/bench -mode local -bench extract -table $(BENCH_TABLE) -iterations $(BENCH_ITERATIONS)

bench-insert:
	@echo "Running insertion benchmarks (local)..."
	@./bench/cmd/bench -mode local -bench insert -table $(BENCH_TABLE) -iterations $(BENCH_ITERATIONS)

bench-cdc:
	@echo "Running CDC benchmarks (local)..."
	@./bench/cmd/bench -mode local -bench cdc -table $(BENCH_TABLE) -iterations $(BENCH_ITERATIONS)

bench-all:
	@echo "Running comprehensive benchmark suite..."
	@mkdir -p bench_results
	@echo "=== Extraction Benchmarks ===" | tee bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@make bench-extract | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "\n=== Insertion Benchmarks ===" | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@make bench-insert | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "\n=== CDC Benchmarks ===" | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@make bench-cdc | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "\n=== Multi-Table Benchmarks ===" | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@make bench-multi | tee -a bench_results/all_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "Results saved to bench_results/"

bench-comparison:
	@echo "Running local vs remote comparison..."
	@mkdir -p bench_results
	@./bench/cmd/bench -mode both -bench all -iterations 30 | tee bench_results/comparison_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "Results saved to bench_results/"

bench-regression:
	@echo "Running regression check..."
	@mkdir -p bench_results
	@if [ -f bench_results/baseline.txt ]; then \
		echo "Comparing against baseline..."; \
		make bench-local > bench_results/current.txt; \
		diff bench_results/baseline.txt bench_results/current.txt || echo "Performance changed!"; \
	else \
		echo "No baseline found. Creating baseline..."; \
		make bench-local > bench_results/baseline.txt; \
		echo "Baseline created at bench_results/baseline.txt"; \
	fi

clean:
	@echo "Cleaning benchmark artifacts..."
	@rm -rf bench_results
	@rm -f bench/cmd/bench
	@psql $(PG_URL) -c "DROP TABLE IF EXISTS bench_data, bench_table_1, bench_table_2, bench_table_3 CASCADE;" 2>/dev/null || true
	@echo "Cleaned!"

# Go benchmark tests (native)
bench-go:
	@echo "Running Go native benchmarks..."
	@go test -bench=. -benchmem -benchtime=10s ./internal/etl/

bench-go-profile:
	@echo "Running benchmarks with CPU profiling..."
	@go test -bench=. -benchmem -cpuprofile=cpu.prof ./internal/etl/
	@echo "View profile with: go tool pprof cpu.prof"

bench-go-mem:
	@echo "Running benchmarks with memory profiling..."
	@go test -bench=. -benchmem -memprofile=mem.prof ./internal/etl/
	@echo "View profile with: go tool pprof mem.prof"

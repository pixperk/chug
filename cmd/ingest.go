package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	ingestPgURL      string
	ingestChURL      string
	ingestTable      string
	ingestTables     string
	ingestLimit      int
	ingestBatch      int
	ingestConfigPath string
	// Polling options
	ingestPoll      bool
	ingestPollDelta string
	ingestPollInt   int
)

type TableResult struct {
	TableName string
	Success   bool
	Error     error
	RowCount  int64
	Duration  time.Duration
}

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest data from PostgreSQL to ClickHouse",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Data Ingestion")
		ui.PrintSubtitle("Transferring data from PostgreSQL to ClickHouse")

		ctx := context.Background()
		log := logx.StyledLog
		log.Info("Starting ingestion process...")

		cfg := loadConfig()

		if cfg.PostgresURL == "" || cfg.ClickHouseURL == "" {
			log.Error("Missing required config values",
				zap.String("pg_url", cfg.PostgresURL),
				zap.String("ch_url", cfg.ClickHouseURL))
			return
		}

		pgConn, err := db.GetPostgresPool(cfg.PostgresURL)
		if err != nil {
			log.Error("Failed to connect to PostgreSQL", zap.Error(err))
			return
		}

		tableConfigs := cfg.GetEffectiveTableConfigs()

		if len(tableConfigs) == 0 {
			log.Error("No tables specified. Use --table, --tables flag, or configure tables in YAML")
			return
		}

		if len(tableConfigs) == 1 {
			ui.PrintSubtitle("Single Table Ingestion")

			resolved := cfg.ResolveTableConfig(tableConfigs[0])

			ui.PrintBox("Configuration",
				fmt.Sprintf("PostgreSQL: Connected\n"+
					"ClickHouse: Connected\n"+
					"Target Table: %s\n"+
					"Batch Size: %s rows\n"+
					"Limit: %s rows",
					resolved.Name,
					ui.HighlightStyle.Render(UI_itoa(resolved.BatchSize)),
					ui.HighlightStyle.Render(UI_itoa(resolved.Limit))))

			result := ingestSingleTable(ctx, cfg, resolved, pgConn)

			if result.Success {
				log.Success("Ingestion completed successfully",
					zap.String("table", result.TableName),
					zap.Int64("rows", result.RowCount))

				if resolved.Polling.Enabled {
					ui.PrintSubtitle("Polling Mode Active")
					select {}
				}
			} else {
				log.Error("Ingestion failed", zap.Error(result.Error))
				os.Exit(1)
			}
		} else {
			ui.PrintSubtitle(fmt.Sprintf("Multi-Table Ingestion (%d tables)", len(tableConfigs)))

			var tableNames []string
			for _, tc := range tableConfigs {
				tableNames = append(tableNames, tc.Name)
			}

			ui.PrintBox("Configuration",
				fmt.Sprintf("PostgreSQL: Connected\n"+
					"ClickHouse: Connected\n"+
					"Tables: %s\n"+
					"Count: %d",
					strings.Join(tableNames, ", "),
					len(tableConfigs)))

			results := ingestMultipleTables(ctx, cfg, pgConn)
			printResultsSummary(results)

			hasPolling := false
			for _, tc := range tableConfigs {
				resolved := cfg.ResolveTableConfig(tc)
				if resolved.Polling.Enabled {
					hasPolling = true
					break
				}
			}

			if hasPolling {
				ui.PrintSubtitle("Polling Mode Active for Some Tables")
				log.Highlight("Running indefinitely - press Ctrl+C to stop")
				select {}
			}
		}
	},
}

// loadConfig loads configuration from file or flags
func loadConfig() *config.Config {
	log := logx.StyledLog

	cfg, err := config.Load(ingestConfigPath)
	if err != nil {
		log.Warn("Could not load config from file, falling back to flags", zap.Error(err))
		cfg = &config.Config{
			PostgresURL:   ingestPgURL,
			ClickHouseURL: ingestChURL,
			Table:         ingestTable,
			Limit:         ingestLimit,
			BatchSize:     ingestBatch,
			Polling: config.PollingConfig{
				Enabled:  ingestPoll,
				DeltaCol: ingestPollDelta,
				Interval: ingestPollInt,
			},
		}
	} else {
		// Override with flags if provided
		if ingestPgURL != "" {
			cfg.PostgresURL = ingestPgURL
		}
		if ingestChURL != "" {
			cfg.ClickHouseURL = ingestChURL
		}
		if ingestTable != "" {
			cfg.Table = ingestTable
		}
		if ingestLimit != 0 {
			cfg.Limit = ingestLimit
		}
		if ingestBatch != 0 {
			cfg.BatchSize = ingestBatch
		}

		if ingestPoll {
			cfg.Polling.Enabled = true
		}
		if ingestPollDelta != "" {
			cfg.Polling.DeltaCol = ingestPollDelta
		}
		if ingestPollInt != 0 {
			cfg.Polling.Interval = ingestPollInt
		}
	}

	// Parse --tables flag (comma-separated list)
	if ingestTables != "" {
		tableNames := strings.Split(ingestTables, ",")
		for _, name := range tableNames {
			trimmed := strings.TrimSpace(name)
			if trimmed != "" {
				cfg.Tables = append(cfg.Tables, config.TableConfig{Name: trimmed})
			}
		}
	}

	// Backward compat: single --table flag
	if ingestTable != "" && len(cfg.Tables) == 0 {
		cfg.Tables = append(cfg.Tables, config.TableConfig{Name: ingestTable})
	}

	return cfg
}

func validateConfig(cfg *config.Config) bool {
	log := logx.StyledLog

	if cfg.PostgresURL == "" || cfg.ClickHouseURL == "" || cfg.Table == "" {
		log.Error("Missing required config values. Provide them in YAML or as flags.",
			zap.String("pg_url", cfg.PostgresURL),
			zap.String("ch_url", cfg.ClickHouseURL),
			zap.String("table", cfg.Table),
		)
		return false
	}

	if cfg.Polling.Enabled {
		if cfg.Polling.DeltaCol == "" {
			log.Error("Missing delta column for polling. Provide it in YAML or with --poll-delta flag.")
			return false
		}
		if cfg.Polling.Interval <= 0 {
			log.Error("Invalid polling interval. Must be greater than 0.")
			return false
		}
	}
	return true
}

func ingestSingleTable(ctx context.Context, cfg *config.Config, tableConfig config.ResolvedTableConfig, pgConn *pgxpool.Pool) TableResult {
	startTime := time.Now()
	result := TableResult{
		TableName: tableConfig.Name,
		Success:   false,
	}

	log := logx.StyledLog.With(zap.String("table", tableConfig.Name))

	log.Info("Extracting data from PostgreSQL (streaming)...")
	stream, err := etl.ExtractTableDataStreaming(ctx, pgConn, tableConfig.Name, &tableConfig.Limit)
	if err != nil {
		result.Error = fmt.Errorf("extraction failed: %w", err)
		return result
	}

	log.Success("Started streaming extraction", zap.Int("columns", len(stream.Columns)))

	log.Info("Building ClickHouse table schema...")
	ddl, err := etl.BuildDDLQuery(tableConfig.Name, stream.Columns)
	if err != nil {
		result.Error = fmt.Errorf("DDL generation failed: %w", err)
		return result
	}

	log.Info("Creating table in ClickHouse...")
	if err := etl.CreateTable(cfg.ClickHouseURL, ddl); err != nil {
		result.Error = fmt.Errorf("table creation failed: %w", err)
		return result
	}

	log.Info("Inserting data into ClickHouse (streaming)...")
	var rowCount atomic.Int64
	rowChan := make(chan []any, 100)

	go func() {
		for row := range stream.RowChan {
			rowChan <- row
			rowCount.Add(1)
		}
		close(rowChan)
	}()

	if err := etl.InsertRowsStreaming(ctx, cfg.ClickHouseURL, tableConfig.Name, etl.GetColumnNames(stream.Columns), rowChan, tableConfig.BatchSize); err != nil {
		result.Error = fmt.Errorf("insertion failed: %w", err)
		return result
	}

	select {
	case err := <-stream.ErrChan:
		if err != nil {
			result.Error = fmt.Errorf("extraction error: %w", err)
			return result
		}
	default:
	}

	result.Success = true
	result.RowCount = rowCount.Load()
	result.Duration = time.Since(startTime)

	log.Success("Ingestion completed", zap.Int64("rows", result.RowCount), zap.Duration("duration", result.Duration))

	if tableConfig.Polling.Enabled {
		go startTablePolling(ctx, cfg, tableConfig, pgConn)
	}

	return result
}

func ingestMultipleTables(ctx context.Context, cfg *config.Config, pgConn *pgxpool.Pool) []TableResult {
	tableConfigs := cfg.GetEffectiveTableConfigs()
	resultChan := make(chan TableResult, len(tableConfigs))
	var wg sync.WaitGroup

	for _, tc := range tableConfigs {
		wg.Add(1)
		go func(tableConfig config.TableConfig) {
			defer wg.Done()
			resolved := cfg.ResolveTableConfig(tableConfig)
			result := ingestSingleTable(ctx, cfg, resolved, pgConn)
			resultChan <- result
		}(tc)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var results []TableResult
	for result := range resultChan {
		results = append(results, result)
	}

	return results
}

func printResultsSummary(results []TableResult) {
	log := logx.StyledLog

	successCount := 0
	failCount := 0
	totalRows := int64(0)

	ui.PrintSubtitle("Ingestion Results")

	for _, r := range results {
		if r.Success {
			successCount++
			totalRows += r.RowCount
			log.Success(
				fmt.Sprintf("Table '%s' completed", r.TableName),
				zap.Int64("rows", r.RowCount),
				zap.Duration("duration", r.Duration),
			)
		} else {
			failCount++
			log.Error(
				fmt.Sprintf("Table '%s' failed", r.TableName),
				zap.Error(r.Error),
			)
		}
	}

	ui.PrintBox("Summary",
		fmt.Sprintf("Total Tables: %d\n", len(results))+
			fmt.Sprintf("Succeeded: %d\n", successCount)+
			fmt.Sprintf("Failed: %d\n", failCount)+
			fmt.Sprintf("Total Rows: %d", totalRows))

	if failCount > 0 {
		os.Exit(1)
	}
}

func startTablePolling(ctx context.Context, cfg *config.Config, tableConfig config.ResolvedTableConfig, pgConn *pgxpool.Pool) {
	log := logx.StyledLog.With(zap.String("table", tableConfig.Name))

	if !tableConfig.Polling.Enabled {
		return
	}

	log.Info("Ensuring index on delta column for efficient polling...")
	if err := etl.EnsureDeltaColumnIndex(ctx, pgConn, tableConfig.Name, tableConfig.Polling.DeltaCol); err != nil {
		log.Warn("Could not create index on delta column", zap.Error(err))
	} else {
		log.Success("Index ready on delta column", zap.String("column", tableConfig.Polling.DeltaCol))
	}

	// Query for the MAX value of the delta column to ensure we start from the correct position
	// This avoids race conditions with streaming ingestion
	var lastSeenValue string
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", tableConfig.Polling.DeltaCol, tableConfig.Name)
	var maxValue any
	if err := pgConn.QueryRow(ctx, query).Scan(&maxValue); err != nil {
		log.Warn("Could not determine max delta value, starting from epoch", zap.Error(err))
		lastSeenValue = "1970-01-01 00:00:00"
	} else if maxValue != nil {
		// Format the max value properly for PostgreSQL
		switch v := maxValue.(type) {
		case time.Time:
			lastSeenValue = v.Format("2006-01-02 15:04:05.999999")
		case string:
			lastSeenValue = v
		case int, int64, int32, int16, int8:
			lastSeenValue = fmt.Sprintf("%d", v)
		case float64, float32:
			lastSeenValue = fmt.Sprintf("%f", v)
		default:
			if t, ok := v.(time.Time); ok {
				lastSeenValue = t.Format("2006-01-02 15:04:05.999999")
			} else {
				lastSeenValue = fmt.Sprintf("%v", v)
			}
		}
	} else {
		// No data in table yet
		lastSeenValue = "1970-01-01 00:00:00"
	}

	pollingCfg := &config.Config{
		PostgresURL:   cfg.PostgresURL,
		ClickHouseURL: cfg.ClickHouseURL,
		Table:         tableConfig.Name,
		BatchSize:     tableConfig.BatchSize,
		Polling:       tableConfig.Polling,
	}

	log.Highlight("Starting poller")
	if err := startPolling(ctx, pollingCfg, lastSeenValue); err != nil && err != context.Canceled {
		log.Error("Poller stopped with error", zap.Error(err))
	}
}

// Helper function to convert int to string for UI
func UI_itoa(n int) string {
	if n == 0 {
		return "all"
	}
	return fmt.Sprintf("%d", n)
}

func init() {
	ingestCmd.Flags().StringVar(&ingestConfigPath, "config", "", "Path to YAML config file (default: .chug.yaml)")
	ingestCmd.Flags().StringVar(&ingestPgURL, "pg-url", "", "PostgreSQL connection URL")
	ingestCmd.Flags().StringVar(&ingestChURL, "ch-url", "", "ClickHouse connection URL")
	ingestCmd.Flags().StringVar(&ingestTable, "table", "", "Table name to ingest")
	ingestCmd.Flags().StringVar(&ingestTables, "tables", "", "Comma-separated list of tables (e.g., users,orders,products)")
	ingestCmd.Flags().IntVar(&ingestLimit, "limit", 1000, "Limit rows to fetch from PG")
	ingestCmd.Flags().IntVar(&ingestBatch, "batch-size", 500, "Rows per ClickHouse insert")
	// Polling flags
	ingestCmd.Flags().BoolVar(&ingestPoll, "poll", false, "Continue polling for changes after initial ingest")
	ingestCmd.Flags().StringVar(&ingestPollDelta, "poll-delta", "", "Column name to track changes (usually a timestamp)")
	ingestCmd.Flags().IntVar(&ingestPollInt, "poll-interval", 0, "Polling interval in seconds")
	rootCmd.AddCommand(ingestCmd)
}

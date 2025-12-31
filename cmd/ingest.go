package cmd

import (
	"context"
	"fmt"
	"strings"

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

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest data from PostgreSQL to ClickHouse",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Data Ingestion")
		ui.PrintSubtitle("Transferring data from PostgreSQL to ClickHouse")

		ctx := context.Background()
		log := logx.StyledLog
		log.Info("Starting ingestion process...")

		// Load configuration from file or flags
		cfg := loadConfig()

		// Validate configuration
		if !validateConfig(cfg) {
			return
		}
		// Show config summary
		ui.PrintBox("Configuration",
			"PostgreSQL: Connected\n"+
				"ClickHouse: Connected\n"+
				"Target Table: "+cfg.Table+"\n"+
				"Batch Size: "+ui.HighlightStyle.Render(UI_itoa(cfg.BatchSize))+" rows\n"+
				"Limit: "+ui.HighlightStyle.Render(UI_itoa(cfg.Limit))+" rows")

		// Connect to PostgreSQL pool
		conn, err := db.GetPostgresPool(cfg.PostgresURL)
		if err != nil {
			log.Error("Failed to connect to PostgreSQL", zap.Error(err))
			return
		}

		// Extract data from PostgreSQL (streaming)
		log.Info("Extracting data from PostgreSQL (streaming)...")
		stream, err := etl.ExtractTableDataStreaming(ctx, conn, cfg.Table, &cfg.Limit)
		if err != nil {
			log.Error("Failed to start extraction from PostgreSQL", zap.Error(err))
			return
		}

		log.Success("Started streaming extraction",
			zap.String("table", cfg.Table),
			zap.Int("columns", len(stream.Columns)))

		// Build DDL query for ClickHouse
		log.Info("Building ClickHouse table schema...")
		ddl, err := etl.BuildDDLQuery(cfg.Table, stream.Columns)
		if err != nil {
			log.Error("Failed to build DDL query", zap.Error(err))
			return
		}

		// Create table in ClickHouse
		log.Info("Creating table in ClickHouse...")
		if err := etl.CreateTable(cfg.ClickHouseURL, ddl); err != nil {
			log.Error("Failed to create table in ClickHouse", zap.Error(err))
			return
		}

		// Insert rows into ClickHouse (streaming)
		log.Info("Inserting data into ClickHouse (streaming)...")

		var lastRow []any
		var lastSeenValue string
		rowChan := make(chan []any, 100)

		go func() {
			for row := range stream.RowChan {
				lastRow = row
				rowChan <- row
			}
			close(rowChan)
		}()

		if err := etl.InsertRowsStreaming(ctx, cfg.ClickHouseURL, cfg.Table, etl.GetColumnNames(stream.Columns), rowChan, cfg.BatchSize); err != nil {
			log.Error("Failed to insert rows into ClickHouse", zap.Error(err))
			return
		}

		select {
		case err := <-stream.ErrChan:
			if err != nil {
				log.Error("Extraction error", zap.Error(err))
				return
			}
		default:
		}

		log.Success("Initial ingest completed successfully",
			zap.String("table", cfg.Table))

		// Start polling if enabled
		if cfg.Polling.Enabled {
			ui.PrintSubtitle("Starting Change Data Polling")

			// Ensure index on delta column
			log.Info("Ensuring index on delta column for efficient polling...")
			if err := etl.EnsureDeltaColumnIndex(ctx, conn, cfg.Table, cfg.Polling.DeltaCol); err != nil {
				log.Warn("Could not create index on delta column (continuing anyway)", zap.Error(err))
			} else {
				log.Success("Index ready on delta column", zap.String("column", cfg.Polling.DeltaCol))
			}

			if lastRow != nil {
				deltaColIndex := -1
				for i, col := range stream.Columns {
					if col.Name == cfg.Polling.DeltaCol {
						deltaColIndex = i
						break
					}
				}

				if deltaColIndex != -1 {
					lastSeenValue = fmt.Sprintf("%v", lastRow[deltaColIndex])
				}
			}

			if err := startPolling(ctx, cfg, lastSeenValue); err != nil {
				log.Error("Polling stopped with error", zap.Error(err))
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

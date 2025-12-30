package cmd

import (
	"context"
	"fmt"

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

		// Extract data from PostgreSQL
		log.Info("Extracting data from PostgreSQL...")
		td, err := etl.ExtractTableData(ctx, conn, cfg.Table, &cfg.Limit)
		if err != nil {
			log.Error("Failed to extract data from PostgreSQL", zap.Error(err))
			return
		}

		log.Success("Extracted table data",
			zap.String("table", cfg.Table),
			zap.Int("rows", len(td.Rows)),
			zap.Int("columns", len(td.Columns)))

		// Build DDL query for ClickHouse
		log.Info("Building ClickHouse table schema...")
		ddl, err := etl.BuildDDLQuery(cfg.Table, td.Columns)
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

		// Insert rows into ClickHouse
		log.Info("Inserting data into ClickHouse...")
		if err := etl.InsertRows(cfg.ClickHouseURL, cfg.Table, etl.GetColumnNames(td.Columns), td.Rows, cfg.BatchSize); err != nil {
			log.Error("Failed to insert rows into ClickHouse", zap.Error(err))
			return
		}

		log.Success("Initial ingest completed successfully",
			zap.String("table", cfg.Table),
			zap.Int("rows", len(td.Rows)))

		// Start polling if enabled
		if cfg.Polling.Enabled {
			ui.PrintSubtitle("Starting Change Data Polling")

			// Determine the last seen value for delta tracking
			lastSeen, err := determineLastSeen(td, cfg.Polling.DeltaCol)
			if err != nil {
				log.Error("Failed to determine last seen value", zap.Error(err))
				return
			}

			// Start polling process
			if err := startPolling(ctx, cfg, lastSeen); err != nil {
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
	ingestCmd.Flags().IntVar(&ingestLimit, "limit", 1000, "Limit rows to fetch from PG")
	ingestCmd.Flags().IntVar(&ingestBatch, "batch-size", 500, "Rows per ClickHouse insert")
	// Polling flags
	ingestCmd.Flags().BoolVar(&ingestPoll, "poll", false, "Continue polling for changes after initial ingest")
	ingestCmd.Flags().StringVar(&ingestPollDelta, "poll-delta", "", "Column name to track changes (usually a timestamp)")
	ingestCmd.Flags().IntVar(&ingestPollInt, "poll-interval", 0, "Polling interval in seconds")
	rootCmd.AddCommand(ingestCmd)
}

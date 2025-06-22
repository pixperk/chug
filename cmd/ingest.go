package cmd

import (
	"context"

	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"github.com/pixperk/chug/internal/logx"
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
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest data from PostgreSQL to ClickHouse",
	Run: func(cmd *cobra.Command, args []string) {

		ctx := context.Background()
		logx.Logger.Info("🚰 Starting ingestion...")
		//Load config if provided
		cfg, err := config.Load(ingestConfigPath)
		if err != nil {
			logx.Logger.Error("⚠️ Could not load config from file, falling back to flags", zap.Error(err))
			cfg = &config.Config{
				PostgresURL:   ingestPgURL,
				ClickHouseURL: ingestChURL,
				Table:         ingestTable,
				Limit:         ingestLimit,
				BatchSize:     ingestBatch,
			}
		} else {
			// Override config values with flags if provided
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
		}

		// Validate config
		if cfg.PostgresURL == "" || cfg.ClickHouseURL == "" || cfg.Table == "" {
			logx.Logger.Fatal("❌ Missing required config values. Provide them in YAML or as flags.",
				zap.String("pg_url", cfg.PostgresURL),
				zap.String("ch_url", cfg.ClickHouseURL),
				zap.String("table", cfg.Table),
			)
			return
		}
		//Connect to PostgreSQL
		conn, err := db.ConnectPostgres(cfg.PostgresURL)
		if err != nil {
			logx.Logger.Error("❌ Failed to connect to PostgreSQL",
				zap.Error(err),
			)

			return
		}

		defer conn.Close(ctx)
		//Extract
		td, err := etl.ExtractTableData(ctx, conn, cfg.Table, &cfg.Limit)
		if err != nil {
			logx.Logger.Error("❌ Failed to extract data from PostgreSQL",
				zap.Error(err),
			)
			return
		}

		logx.Logger.Info("📦 Extracted table",
			zap.String("table", cfg.Table),
			zap.Int("rows", len(td.Rows)),
			zap.Int("columns", len(td.Columns)),
		)

		//Transform
		ddl, err := etl.BuildDDLQuery(cfg.Table, td.Columns)
		if err != nil {
			logx.Logger.Error("❌ Failed to build DDL query",
				zap.Error(err))
			return
		}
		//Load
		//Connect to ClickHouse
		chConn, err := db.ConnectClickHouse(cfg.ClickHouseURL)
		if err != nil {
			logx.Logger.Error("❌ Failed to connect to ClickHouse",
				zap.Error(err),
			)
			return
		}
		defer chConn.Close()
		//Create table
		if err := etl.CreateTable(cfg.ClickHouseURL, ddl); err != nil {
			logx.Logger.Error("❌ Failed to create table in ClickHouse",
				zap.Error(err),
			)
			return
		}

		//Insert rows
		if err := etl.InsertRows(cfg.ClickHouseURL, cfg.Table, etl.GetColumnNames(td.Columns), td.Rows, cfg.BatchSize); err != nil {
			logx.Logger.Error("❌ Failed to insert rows into ClickHouse",
				zap.Error(err),
			)
			return
		}

	},
}

func init() {
	ingestCmd.Flags().StringVar(&ingestConfigPath, "config", "", "Path to YAML config file (default: .chug.yaml)")
	ingestCmd.Flags().StringVar(&ingestPgURL, "pg-url", "", "Postgres connection URL")
	ingestCmd.Flags().StringVar(&ingestChURL, "ch-url", "", "ClickHouse connection URL")
	ingestCmd.Flags().StringVar(&ingestTable, "table", "", "Table name to ingest")
	ingestCmd.Flags().IntVar(&ingestLimit, "limit", 1000, "Limit rows to fetch from PG")
	ingestCmd.Flags().IntVar(&ingestBatch, "batch-size", 500, "Rows per ClickHouse insert")

	rootCmd.AddCommand(ingestCmd)
}

package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/etl/export"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	exportTable      string
	exportFormat     string
	exportOut        string
	exportChURL      string
	exportConfigPath string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export data from ClickHouse to CSV",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Exporting Data")
		ui.PrintSubtitle("Converting ClickHouse table to external format")

		log := logx.StyledLog
		log.Info("Starting export process...")

		// Load config if provided
		cfg, err := config.Load(exportConfigPath)
		if err != nil {
			log.Warn("Could not load config from file, falling back to flags", zap.Error(err))
			cfg = &config.Config{
				ClickHouseURL: exportChURL,
				Table:         exportTable,
			}
		} else {
			// Override config values with flags if provided
			if exportChURL != "" {
				cfg.ClickHouseURL = exportChURL
			}
			if exportTable != "" {
				cfg.Table = exportTable
			}
		}

		// Validate config
		if cfg.ClickHouseURL == "" || cfg.Table == "" {
			log.Fatal("Missing required config values. Provide them in YAML or as flags.",
				zap.String("ch_url", cfg.ClickHouseURL),
				zap.String("table", cfg.Table),
			)
			return
		}

		if exportFormat != "csv" {
			log.Error("Unsupported export format. Currently only CSV is supported.")
			return
		}

		// Show export details
		ui.PrintBox("Export Details", fmt.Sprintf(
			"Table: %s\nFormat: %s\nOutput Directory: %s",
			cfg.Table, exportFormat, exportOut,
		))

		outPath := filepath.Join(exportOut, fmt.Sprintf("%s.%s", cfg.Table, exportFormat))

		log.Info("Starting data extraction...")
		if err := export.ExportTableToCSV(cfg.ClickHouseURL, cfg.Table, outPath); err != nil {
			log.Error("Export failed",
				zap.String("table", cfg.Table),
				zap.String("format", exportFormat),
				zap.String("output", outPath),
				zap.Error(err),
			)
			return
		}

		log.Success("Export completed successfully")
		ui.PrintBox("Export Result", fmt.Sprintf(
			"Table: %s\nFormat: %s\nOutput File: %s",
			cfg.Table, exportFormat, outPath,
		))
	},
}

func init() {
	exportCmd.Flags().StringVar(&exportConfigPath, "config", "", "Path to YAML config file (default: .chug.yaml)")
	exportCmd.Flags().StringVar(&exportChURL, "ch-url", "", "ClickHouse connection URL")
	exportCmd.Flags().StringVar(&exportTable, "table", "", "Table name to export")
	exportCmd.Flags().StringVar(&exportFormat, "format", "csv", "Export format (currently only csv is supported)")
	exportCmd.Flags().StringVar(&exportOut, "out", ".", "Output directory for exported files")

	exportCmd.MarkFlagRequired("format")
	exportCmd.MarkFlagRequired("out")

	rootCmd.AddCommand(exportCmd)
}

package cmd

import (
	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	connectConfigPath string
)

var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Test Postgres and ClickHouse connections",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Testing Database Connections")
		ui.PrintSubtitle("Checking connectivity to PostgreSQL and ClickHouse")

		log := logx.StyledLog

		// Load config from YAML file
		cfg, err := config.Load(connectConfigPath)
		if err != nil {
			log.Error("Failed to load config", zap.Error(err))
			return
		}

		// Use config values
		pgURL := cfg.PostgresURL
		chURL := cfg.ClickHouseURL

		if pgURL == "" {
			log.Error("PostgreSQL URL not provided (set pg_url in .chug.yaml)")
			return
		}
		if chURL == "" {
			log.Error("ClickHouse URL not provided (set ch_url in .chug.yaml)")
			return
		}

		log.Info("Testing PostgreSQL connection...")
		_, err = db.GetPostgresPool(pgURL)
		if err != nil {
			log.Error("PostgreSQL connection failed", zap.Error(err))
			return
		}
		log.Success("PostgreSQL connected successfully")

		log.Info("Testing ClickHouse connection...")
		_, err = db.GetClickHousePool(chURL)
		if err != nil {
			log.Error("ClickHouse connection failed", zap.Error(err))
			return
		}
		log.Success("ClickHouse connected successfully")

		ui.PrintBox("Connection Status", "Both database connections are working correctly.")
	},
}

func init() {
	connectCmd.Flags().StringVar(&connectConfigPath, "config", ".chug.yaml", "Path to config file")
	rootCmd.AddCommand(connectCmd)
}

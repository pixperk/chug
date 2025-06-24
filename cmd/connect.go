package cmd

import (
	"context"

	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	pgURL string
	chURL string
)

var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Test Postgres and ClickHouse connections",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Testing Database Connections")
		ui.PrintSubtitle("Checking connectivity to PostgreSQL and ClickHouse")

		log := logx.StyledLog

		log.Info("Testing PostgreSQL connection...")
		conn, err := db.ConnectPostgres(pgURL)
		if err != nil {
			log.Error("PostgreSQL connection failed", zap.Error(err))
			return
		}
		defer conn.Close(context.Background())
		log.Success("PostgreSQL connected successfully")

		log.Info("Testing ClickHouse connection...")
		chConn, err := db.ConnectClickHouse(chURL)
		if err != nil {
			log.Error("ClickHouse connection failed", zap.Error(err))
			return
		}
		defer chConn.Close()
		log.Success("ClickHouse connected successfully")

		ui.PrintBox("Connection Status", "Both database connections are working correctly.")
	},
}

func init() {
	connectCmd.Flags().StringVar(&pgURL, "pg-url", "", "PostgreSQL connection string")
	connectCmd.Flags().StringVar(&chURL, "ch-url", "", "ClickHouse connection URL")
	connectCmd.MarkFlagRequired("pg-url")
	connectCmd.MarkFlagRequired("ch-url")
	rootCmd.AddCommand(connectCmd)
}

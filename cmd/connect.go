package cmd

import (
	"context"

	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/logx"
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
		logx.Logger.Info("Testing Postgres connection...")
		conn, err := db.ConnectPostgres(pgURL)
		if err != nil {
			logx.Logger.Error("❌ Postgres connection failed",
				zap.Error(err),
			)
			return
		}
		defer conn.Close(context.Background())
		logx.Logger.Info("✅ Postgres connected")
		logx.Logger.Info("Testing ClickHouse connection...")
		chConn, err := db.ConnectClickHouse(chURL)
		if err != nil {
			logx.Logger.Error("❌ ClickHouse connection failed",
				zap.Error(err),
			)
			return
		}
		defer chConn.Close()
		logx.Logger.Info("✅ ClickHouse connected")
	},
}

func init() {
	connectCmd.Flags().StringVar(&pgURL, "pg-url", "", "Postgres connection string")
	connectCmd.Flags().StringVar(&chURL, "ch-url", "", "ClickHouse connection URL")
	connectCmd.MarkFlagRequired("pg-url")
	connectCmd.MarkFlagRequired("ch-url")
	rootCmd.AddCommand(connectCmd)
}

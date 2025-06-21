package cmd

import (
	"os"

	"github.com/pixperk/chug/internal/logx"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   "chug",
	Short: "Chug is a blazing-fast ETL pipeline from Postgres to ClickHouse",
	Long: `Chug streams data from your Postgres tables into ClickHouse 
for analytics at ludicrous speed. 
Just point, chug, and ask.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logx.Logger.Error("❌ Command execution failed",
			zap.Error(err),
		)

		os.Exit(1)
	}
}

func init() {
	//TODO
}

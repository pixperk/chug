package cmd

import (
	"context"
	"fmt"

	"github.com/pixperk/chug/internal/db"
	"github.com/spf13/cobra"
)

var (
	pgURL string
	chURL string
)

var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Test Postgres and ClickHouse connections",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Testing Postgres connection...")
		conn, err := db.ConnectPostgres(pgURL)
		if err != nil {
			fmt.Println("❌ Postgres connection failed:", err)
			return
		}
		defer conn.Close(context.Background())
		fmt.Println("✅ Postgres connected")
		fmt.Println("Testing ClickHouse connection...")
		chConn, err := db.ConnectClickHouse(chURL)
		if err != nil {
			fmt.Println("❌ ClickHouse connection failed:", err)
			return
		}
		defer chConn.Close()
		fmt.Println("✅ ClickHouse connected")
	},
}

func init() {
	connectCmd.Flags().StringVar(&pgURL, "pg-url", "", "Postgres connection string")
	connectCmd.Flags().StringVar(&chURL, "ch-url", "", "ClickHouse connection URL")
	connectCmd.MarkFlagRequired("pg-url")
	connectCmd.MarkFlagRequired("ch-url")
	rootCmd.AddCommand(connectCmd)
}

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func init() {
	//TODO
}

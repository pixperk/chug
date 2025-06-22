package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var sampleConfigCmd = &cobra.Command{
	Use:   "sample-config",
	Short: "Generate a sample config file (.chug.yaml)", Run: func(cmd *cobra.Command, args []string) {
		const sample = `# chug.yaml - Sample Configuration

# PostgreSQL connection URL
pg_url: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable"

# ClickHouse HTTP interface URL
ch_url: "http://localhost:9000"

# Table to ingest from Postgres
table: UserAnswer

# Max rows to fetch
limit: 1000

# Batch size per insert
batch_size: 200

# Polling configuration
polling:
  # Enable polling for changes after initial ingest
  enabled: false
  # Column name to track changes (usually a timestamp)
  delta_column: "updated_at"
  # Polling interval in seconds
  interval_seconds: 30
`
		err := os.WriteFile(".chug.yaml", []byte(sample), 0644)
		if err != nil {
			fmt.Printf("❌ Failed to write .chug.yaml: %v\n", err)
			return
		}
		fmt.Println("✅ Sample config written to .chug.yaml")
	},
}

func init() {
	rootCmd.AddCommand(sampleConfigCmd)
}

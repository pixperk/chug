package cmd

import (
	"os"

	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var sampleConfigCmd = &cobra.Command{
	Use:   "sample-config",
	Short: "Generate a sample config file (.chug.yaml)",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Configuration Generator")
		ui.PrintSubtitle("Creating a sample configuration file")

		log := logx.StyledLog

		const sample = `# chug.yaml - Sample Configuration

# PostgreSQL connection URL
pg_url: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable"

# ClickHouse connection URL
# Local: http://localhost:9000
# Cloud: https://username:password@host.clickhouse.cloud:9440
ch_url: "http://localhost:9000"

# --- Single Table Mode ---
# Uncomment to ingest a single table
# table: "users"
# limit: 1000          # Max rows (0 = unlimited)
# batch_size: 500      # Rows per batch
# polling:
#   enabled: false
#   delta_column: "updated_at"
#   interval_seconds: 30

# --- Multi-Table Mode (Recommended) ---
# Comment out 'table' above and use 'tables' below for multiple tables

# Global defaults (apply to all tables unless overridden)
limit: 0
batch_size: 500

tables:
  # Simple table (uses global defaults)
  - name: "users"

  # Table with custom batch size
  - name: "orders"
    batch_size: 1000

  # Table with polling enabled
  - name: "events"
    polling:
      enabled: true
      delta_column: "updated_at"
      interval_seconds: 60

  # Table with custom limit
  - name: "products"
    limit: 10000
`
		log.Info("Creating sample configuration file...")

		err := os.WriteFile(".chug.yaml", []byte(sample), 0644)
		if err != nil {
			log.Error("Failed to write .chug.yaml", zap.Error(err))
			return
		}

		log.Success("Sample config written to .chug.yaml")

		ui.PrintBox("Next Steps",
			"1. Edit .chug.yaml with your database credentials\n"+
				"2. Choose single-table or multi-table mode\n"+
				"3. Configure your tables and settings\n"+
				"4. Run 'chug ingest' to start data transfer")
	},
}

func init() {
	rootCmd.AddCommand(sampleConfigCmd)
}

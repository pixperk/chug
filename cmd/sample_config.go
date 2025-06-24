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
		log.Info("Creating sample configuration file...")

		err := os.WriteFile(".chug.yaml", []byte(sample), 0644)
		if err != nil {
			log.Error("Failed to write .chug.yaml", zap.Error(err))
			return
		}

		log.Success("Sample config written to .chug.yaml")

		ui.PrintBox("Next Steps",
			"1. Edit .chug.yaml with your database credentials\n"+
				"2. Configure your table and polling settings\n"+
				"3. Run 'chug ingest' to start data transfer")
	},
}

func init() {
	rootCmd.AddCommand(sampleConfigCmd)
}

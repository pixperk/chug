package cmd

import (
	"github.com/pixperk/chug/api"
	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/ui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	servePort       string
	serveConfigPath string
	servePgURL      string
	serveChURL      string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the API server for web-based ingestion",
	Long:  `Starts a REST API server with WebSocket support for real-time ingestion monitoring`,
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintTitle("Chug API Server")
		ui.PrintSubtitle("REST API with WebSocket support for real-time monitoring")

		log := logx.StyledLog

		// Load config
		cfg, err := config.Load(serveConfigPath)
		if err != nil {
			log.Warn("Could not load config from file, using flags", zap.Error(err))
			cfg = &config.Config{}
		}

		// Override with flags if provided
		if servePgURL != "" {
			cfg.PostgresURL = servePgURL
		}
		if serveChURL != "" {
			cfg.ClickHouseURL = serveChURL
		}

		// Validate config
		if cfg.PostgresURL == "" || cfg.ClickHouseURL == "" {
			log.Error("Missing required configuration",
				zap.String("pg_url", cfg.PostgresURL),
				zap.String("ch_url", cfg.ClickHouseURL))
			log.Info("Use --pg-url and --ch-url flags or provide a config file")
			return
		}

		ui.PrintBox("Configuration",
			"PostgreSQL: Connected\n"+
				"ClickHouse: Connected\n"+
				"Server Port: "+servePort)

		// Create and start server
		server := api.NewServer(cfg, log.GetZapLogger())

		log.Highlight("Starting API server on http://localhost:" + servePort)
		log.Info("Endpoints available:")
		log.Info("  GET  /health                - Health check")
		log.Info("  POST /api/v1/ingest         - Start ingestion job")
		log.Info("  GET  /api/v1/jobs           - List all jobs")
		log.Info("  GET  /api/v1/jobs/{id}      - Get job status")
		log.Info("  WS   /ws                    - WebSocket for real-time updates")
		log.Highlight("Press Ctrl+C to stop")

		if err := server.Start(":" + servePort); err != nil {
			log.Error("Server failed", zap.Error(err))
		}
	},
}

func init() {
	serveCmd.Flags().StringVar(&servePort, "port", "8080", "Port to run the API server on")
	serveCmd.Flags().StringVar(&serveConfigPath, "config", "", "Path to YAML config file")
	serveCmd.Flags().StringVar(&servePgURL, "pg-url", "", "PostgreSQL connection URL")
	serveCmd.Flags().StringVar(&serveChURL, "ch-url", "", "ClickHouse connection URL")
	rootCmd.AddCommand(serveCmd)
}

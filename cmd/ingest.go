package cmd

import (
	"context"

	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"github.com/pixperk/chug/internal/logx"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	ingestPgURL string
	ingestChURL string
	ingestTable string
	ingestLimit int
	ingestBatch int
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest data from PostgreSQL to ClickHouse", Run: func(cmd *cobra.Command, args []string) {

		ctx := context.Background()
		logx.Logger.Info("🚰 Starting ingestion...")

		//Connect to PostgreSQL
		conn, err := db.ConnectPostgres(ingestPgURL)
		if err != nil {
			logx.Logger.Error("❌ Failed to connect to PostgreSQL",
				zap.Error(err),
			)

			return
		}

		defer conn.Close(ctx)

		//Extract
		td, err := etl.ExtractTableData(ctx, conn, ingestTable, &ingestLimit)
		if err != nil {
			logx.Logger.Error("❌ Failed to extract data from PostgreSQL",

				zap.Error(err),
			)
			return
		}
		logx.Logger.Info("📦 Extracted table",
			zap.String("table", ingestTable),
			zap.Int("rows", len(td.Rows)),
			zap.Int("columns", len(td.Columns)),
		)

		//Transform
		ddl, err := etl.BuildDDLQuery(ingestTable, td.Columns)
		if err != nil {
			logx.Logger.Error("❌ Failed to build DDL query",
				zap.Error(err))
			return
		}

		//Load
		//Connect to ClickHouse
		chConn, err := db.ConnectClickHouse(ingestChURL)
		if err != nil {
			logx.Logger.Error("❌ Failed to connect to ClickHouse",
				zap.Error(err),
			)
			return
		}
		defer chConn.Close()
		//Create table
		if err := etl.CreateTable(ingestChURL, ddl); err != nil {
			logx.Logger.Error("❌ Failed to create table in ClickHouse",
				zap.Error(err),
			)
			return
		}

		//Insert rows
		if err := etl.InsertRows(ingestChURL, ingestTable, etl.GetColumnNames(td.Columns), td.Rows, ingestBatch); err != nil {
			logx.Logger.Error("❌ Failed to insert rows into ClickHouse",
				zap.Error(err),
			)
			return
		}

	},
}

func init() {
	ingestCmd.Flags().StringVar(&ingestPgURL, "pg-url", "", "Postgres connection URL")
	ingestCmd.Flags().StringVar(&ingestChURL, "ch-url", "", "ClickHouse connection URL")
	ingestCmd.Flags().StringVar(&ingestTable, "table", "", "Table name to ingest")
	ingestCmd.Flags().IntVar(&ingestLimit, "limit", 1000, "Limit rows to fetch from PG")
	ingestCmd.Flags().IntVar(&ingestBatch, "batch-size", 500, "Rows per ClickHouse insert")

	ingestCmd.MarkFlagRequired("pg-url")
	ingestCmd.MarkFlagRequired("ch-url")
	ingestCmd.MarkFlagRequired("table")

	rootCmd.AddCommand(ingestCmd)
}

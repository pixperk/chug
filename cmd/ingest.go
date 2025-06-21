package cmd

import (
	"context"
	"fmt"

	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"github.com/spf13/cobra"
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
	Short: "Ingest data from PostgreSQL to ClickHouse",
	Run: func(cmd *cobra.Command, args []string) {

		ctx := context.Background()
		fmt.Println("🚰 Starting ingestion...")

		//Connect to PostgreSQL
		conn, err := db.ConnectPostgres(ingestPgURL)
		if err != nil {
			fmt.Printf("❌ Failed to connect to PostgreSQL: %v\n", err)
			return
		}

		defer conn.Close(ctx)

		//Extract
		td, err := etl.ExtractTableData(ctx, conn, ingestTable)
		if err != nil {
			fmt.Printf("❌ Failed to extract data from PostgreSQL: %v\n", err)
			return
		}

		//Transform
		ddl, err := etl.BuildDDLQuery(ingestTable, td.Columns)
		if err != nil {
			fmt.Printf("❌ Failed to build DDL query: %v\n", err)
			return
		}

		//Load
		//Connect to ClickHouse
		chConn, err := db.ConnectClickHouse(ingestChURL)
		if err != nil {
			fmt.Printf("❌ Failed to connect to ClickHouse: %v\n", err)
			return
		}
		defer chConn.Close()
		//Create table
		if err := etl.CreateTable(ingestChURL, ddl); err != nil {
			fmt.Printf("❌ Failed to create table in ClickHouse: %v\n", err)
			return
		}

		//Insert rows
		if err := etl.InsertRows(ingestChURL, ingestTable, etl.GetColumnNames(td.Columns), td.Rows, ingestBatch); err != nil {
			fmt.Printf("❌ Failed to insert rows into ClickHouse: %v\n", err)
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

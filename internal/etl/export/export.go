package export

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"github.com/pixperk/chug/internal/logx"
	"go.uber.org/zap"
)

func ExportTableToCSV(table, chUrl, outPath string) error {
	conn, err := db.ConnectClickHouse(chUrl)
	if err != nil {
		return fmt.Errorf("❌ ClickHouse connection failed: %w", err)
	}
	defer conn.Close()

	exists, err := TableExists(conn, table)
	if err != nil {
		return fmt.Errorf("⚠️ Could not verify table: %w", err)
	}
	if !exists {
		return fmt.Errorf("🚫 Table '%s' does not exist", table)
	}
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", etl.QuoteIdentifier(table)))
	if err != nil {
		return fmt.Errorf("❌ Query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("❌ Could not get columns: %w", err)
	}
	if len(cols) == 0 {
		return fmt.Errorf("🚫 No columns found in table '%s'", table)
	}

	file, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("❌ Could not create output file '%s': %w", outPath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	//write header
	if err := writer.Write(cols); err != nil {
		return fmt.Errorf("❌ Could not write header to CSV: %w", err)
	}

	count := 0
	for rows.Next() {
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return fmt.Errorf("❌ Could not read row values: %w", err)
		}
		record := make([]string, len(cols))
		for i, col := range columns {
			if col == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", col)
			}
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("❌ Could not write row to CSV: %w", err)
		}
		count++
		if count%500 == 0 {
			logx.Logger.Info("Exported rows...\n",
				zap.Int("rows", count),
				zap.String("table", table),
			)
		}
	}

	logx.Logger.Info("✅ Exported table to CSV",
		zap.String("table", table),
		zap.Int("rows", count),
		zap.String("output", outPath),
	)
	return nil

}

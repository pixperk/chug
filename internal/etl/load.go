package etl

import (
	"context"
	"fmt"
	"time"

	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/logx"
	"go.uber.org/zap"
)

func CreateTable(chURL, ddl string) error {
	conn, err := db.GetClickHousePool(chURL)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(context.Background(), ddl)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	logx.Logger.Info("Table created successfully")
	return nil
}

func InsertRows(chURL, table string, columns []string, rows [][]any, batchSize int) error {
	// Validate table name as an extra security measure
	if !IsValidIdentifier(table) {
		return fmt.Errorf("invalid table name: %s", table)
	}

	// Validate column names as an extra security measure
	for _, col := range columns {
		if !IsValidIdentifier(col) {
			return fmt.Errorf("invalid column name: %s", col)
		}
	}

	conn, err := db.GetClickHousePool(chURL)
	if err != nil {
		return err
	}

	// Quote each column name to prevent SQL injection
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = QuoteIdentifier(col)
	}

	colNames := "(" + join(quotedColumns, ", ") + ")"

	// Use QuoteIdentifier to safely quote the table name
	quotedTable := QuoteIdentifier(table)
	insertPrefix := fmt.Sprintf("INSERT INTO %s %s VALUES ", quotedTable, colNames)

	ctx := context.Background()
	for i := 0; i < len(rows); i += batchSize {
		end := min(i+batchSize, len(rows))
		batch := rows[i:end]

		query := insertPrefix + buildValuesPlaceholders(len(batch), len(columns))
		args := flatten(batch)

		err := Retry(ctx, RetryConfig{
			MaxAttempts: 4,
			BaseDelay:   250 * time.Millisecond,
			MaxDelay:    2 * time.Second,
			Jitter:      true,
		}, func() error {
			_, err := conn.ExecContext(ctx, query, args...)
			return err
		})

		if err != nil {
			return fmt.Errorf("failed to insert rows into %s: %w", table, err)
		}

		logx.Logger.Info("Inserted rows into ClickHouse",
			zap.Int("row_count", end-i),
			zap.String("table", table),
			zap.Int("batch_size", batchSize),
			zap.Int("total_rows", len(rows)),
		)

	}
	return nil
}

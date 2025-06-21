package etl

import (
	"context"
	"fmt"

	"github.com/pixperk/chug/internal/db"
)

func CreateTable(chURL, ddl string) error {
	conn, err := db.ConnectClickHouse(chURL)
	if err != nil {
		return err
	}

	defer conn.Close()

	// DDL statements should be validated separately since they're more complex
	// Here we're assuming the DDL is trusted input or has been validated elsewhere
	_, err = conn.ExecContext(context.Background(), ddl)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Println("Table created successfully")
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

	conn, err := db.ConnectClickHouse(chURL)
	if err != nil {
		return err
	}
	defer conn.Close()
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

		_, err = conn.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert rows into %s: %w", table, err)
		}
		fmt.Printf("Inserted %d rows into %s\n", len(batch), table)
	}
	return nil
}

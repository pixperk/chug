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
	_, err = conn.ExecContext(context.Background(), ddl)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Println("Table created successfully")
	return nil
}

func InsertRows(chURL, table string, columns []string, rows [][]any, batchSize int) error {
	conn, err := db.ConnectClickHouse(chURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	colNames := "(" + join(columns, ", ") + ")"
	insertPrefix := fmt.Sprintf("INSERT INTO %s %s VALUES ", table, colNames)

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

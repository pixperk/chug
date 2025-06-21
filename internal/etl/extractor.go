package etl

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Column struct {
	Name string
	Type string
}

type TableData struct {
	Columns []Column
	Rows    [][]any
}

func ExtractTableData(ctx context.Context, conn *pgx.Conn, table string) (*TableData, error) {

	//Get column information
	colQuery := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := conn.Query(ctx, colQuery, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}

	defer rows.Close()

	var cols []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.Type); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}
	//Get table data
	dataQuery := "SELECT * FROM " + pgx.Identifier{table}.Sanitize()
	dataRows, err := conn.Query(ctx, dataQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query table data: %w", err)
	}
	defer dataRows.Close()

	var result [][]any
	for dataRows.Next() {
		values, err := dataRows.Values()
		if err != nil {
			return nil, fmt.Errorf("failed to get row values: %w", err)
		}
		result = append(result, values)
	}
	if err := dataRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table data: %w", err)
	}

	return &TableData{
		Columns: cols,
		Rows:    result,
	}, nil

}

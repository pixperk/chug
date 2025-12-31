package etl

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Column struct {
	Name string
	Type string
}

type TableData struct {
	Columns []Column
	Rows    [][]any
}

func getColumns(ctx context.Context, conn *pgxpool.Pool, table string) ([]Column, error) {
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
	return cols, nil
}

func ExtractTableData(ctx context.Context, conn *pgxpool.Pool, table string, limit *int) (*TableData, error) {
	cols, err := getColumns(ctx, conn, table)
	if err != nil {
		return nil, err
	}

	var query string
	var rows pgx.Rows

	if limit != nil && *limit > 0 {
		query = "SELECT * FROM " + pgx.Identifier{table}.Sanitize() + " LIMIT $1"
		rows, err = conn.Query(ctx, query, *limit)
	} else {
		query = "SELECT * FROM " + pgx.Identifier{table}.Sanitize()
		rows, err = conn.Query(ctx, query)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query table data: %w", err)
	}
	defer rows.Close()

	var result [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("failed to get row values: %w", err)
		}
		for i, val := range values {
			var uuidBytes []byte
			switch v := val.(type) {
			case [16]byte:
				uuidBytes = v[:]
			case []byte:
				uuidBytes = v
			}

			if uuidBytes != nil && (cols[i].Type == "uuid" || cols[i].Type == "bytea") {
				if len(uuidBytes) == 16 {
					// Format byte slice as UUID string
					values[i] = fmt.Sprintf("%x-%x-%x-%x-%x", uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:16])
				}
			}
		}
		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &TableData{
		Columns: cols,
		Rows:    result,
	}, nil
}

// polling use case
func ExtractTableDataSince(ctx context.Context, conn *pgxpool.Pool, table, deltaCol, lastSeen string, limit *int) (*TableData, error) {
	cols, err := getColumns(ctx, conn, table)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC",
		pgx.Identifier{table}.Sanitize(),
		deltaCol,
		deltaCol,
	)

	var rows pgx.Rows
	if limit != nil && *limit > 0 {
		query += " LIMIT $2"
		rows, err = conn.Query(ctx, query, lastSeen, *limit)
	} else {
		rows, err = conn.Query(ctx, query, lastSeen)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query delta rows: %w", err)
	}
	defer rows.Close()

	var result [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("failed to get delta row values: %w", err)
		}
		for i, val := range values {
			var uuidBytes []byte
			switch v := val.(type) {
			case [16]byte:
				uuidBytes = v[:]
			case []byte:
				uuidBytes = v
			}

			if uuidBytes != nil && (cols[i].Type == "uuid" || cols[i].Type == "bytea") {
				if len(uuidBytes) == 16 {
					// Format byte slice as UUID string
					values[i] = fmt.Sprintf("%x-%x-%x-%x-%x", uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:16])
				}
			}
		}
		result = append(result, values)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating delta rows: %w", err)
	}

	return &TableData{
		Columns: cols,
		Rows:    result,
	}, nil
}

func GetColumnNames(cols []Column) []string {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	return names
}

func EnsureDeltaColumnIndex(ctx context.Context, conn *pgxpool.Pool, table, deltaCol string) error {
	indexName := fmt.Sprintf("idx_%s_%s_chug", table, deltaCol)

	checkQuery := `
		SELECT COUNT(*)
		FROM pg_indexes
		WHERE tablename = $1
		AND indexname = $2
	`

	var count int
	err := conn.QueryRow(ctx, checkQuery, table, indexName).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check for index: %w", err)
	}

	if count > 0 {
		return nil
	}

	createQuery := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
		pgx.Identifier{indexName}.Sanitize(),
		pgx.Identifier{table}.Sanitize(),
		pgx.Identifier{deltaCol}.Sanitize(),
	)

	_, err = conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create index on %s.%s: %w", table, deltaCol, err)
	}

	return nil
}

type StreamResult struct {
	Columns []Column
	RowChan <-chan []any
	ErrChan <-chan error
}

func ExtractTableDataStreaming(ctx context.Context, conn *pgxpool.Pool, table string, limit *int) (*StreamResult, error) {
	cols, err := getColumns(ctx, conn, table)
	if err != nil {
		return nil, err
	}

	rowChan := make(chan []any, 100)
	errChan := make(chan error, 1)

	go func() {
		defer close(rowChan)
		defer close(errChan)

		var query string
		var rows pgx.Rows
		var err error

		if limit != nil && *limit > 0 {
			query = "SELECT * FROM " + pgx.Identifier{table}.Sanitize() + " LIMIT $1"
			rows, err = conn.Query(ctx, query, *limit)
		} else {
			query = "SELECT * FROM " + pgx.Identifier{table}.Sanitize()
			rows, err = conn.Query(ctx, query)
		}
		if err != nil {
			errChan <- fmt.Errorf("failed to query table data: %w", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				errChan <- fmt.Errorf("failed to get row values: %w", err)
				return
			}

			for i, val := range values {
				var uuidBytes []byte
				switch v := val.(type) {
				case [16]byte:
					uuidBytes = v[:]
				case []byte:
					uuidBytes = v
				}

				if uuidBytes != nil && (cols[i].Type == "uuid" || cols[i].Type == "bytea") {
					if len(uuidBytes) == 16 {
						values[i] = fmt.Sprintf("%x-%x-%x-%x-%x", uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:16])
					}
				}
			}

			select {
			case rowChan <- values:
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}

		if err := rows.Err(); err != nil {
			errChan <- fmt.Errorf("error iterating rows: %w", err)
		}
	}()

	return &StreamResult{
		Columns: cols,
		RowChan: rowChan,
		ErrChan: errChan,
	}, nil
}

func ExtractTableDataSinceStreaming(ctx context.Context, conn *pgxpool.Pool, table, deltaCol, lastSeen string, limit *int) (*StreamResult, error) {
	cols, err := getColumns(ctx, conn, table)
	if err != nil {
		return nil, err
	}

	rowChan := make(chan []any, 100)
	errChan := make(chan error, 1)

	go func() {
		defer close(rowChan)
		defer close(errChan)

		query := fmt.Sprintf(
			"SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC",
			pgx.Identifier{table}.Sanitize(),
			deltaCol,
			deltaCol,
		)

		var rows pgx.Rows
		var err error
		if limit != nil && *limit > 0 {
			query += " LIMIT $2"
			rows, err = conn.Query(ctx, query, lastSeen, *limit)
		} else {
			rows, err = conn.Query(ctx, query, lastSeen)
		}
		if err != nil {
			errChan <- fmt.Errorf("failed to query delta rows: %w", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				errChan <- fmt.Errorf("failed to get delta row values: %w", err)
				return
			}

			for i, val := range values {
				var uuidBytes []byte
				switch v := val.(type) {
				case [16]byte:
					uuidBytes = v[:]
				case []byte:
					uuidBytes = v
				}

				if uuidBytes != nil && (cols[i].Type == "uuid" || cols[i].Type == "bytea") {
					if len(uuidBytes) == 16 {
						values[i] = fmt.Sprintf("%x-%x-%x-%x-%x", uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:16])
					}
				}
			}

			select {
			case rowChan <- values:
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}

		if err := rows.Err(); err != nil {
			errChan <- fmt.Errorf("error iterating delta rows: %w", err)
		}
	}()

	return &StreamResult{
		Columns: cols,
		RowChan: rowChan,
		ErrChan: errChan,
	}, nil
}

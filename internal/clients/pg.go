package clients

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/pixperk/chug/internal/etl"
)

type PostgresClient interface {
	FetchNewRows(ctx context.Context, table, cursorColumn string, lastCursor any, limit int) ([]map[string]any, error)
	Close() error
}

type client struct {
	db *sql.DB
}

func New(connStr string) (PostgresClient, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &client{db: db}, nil
}

func (c *client) FetchNewRows(ctx context.Context, table, cursorColumn string, lastCursor any, limit int) ([]map[string]any, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC LIMIT $2`, etl.QuoteIdentifier(table), etl.QuoteIdentifier(cursorColumn), etl.QuoteIdentifier(cursorColumn))
	rows, err := c.db.QueryContext(ctx, query, lastCursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := map[string]any{}
		for i, col := range cols {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (c *client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgres(pgURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	return conn.Ping(context.Background())
}

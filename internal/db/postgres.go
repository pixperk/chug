package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

func ConnectPostgres(pgURL string) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return pgx.Connect(ctx, pgURL)

}

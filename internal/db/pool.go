package db

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	pgPool *pgxpool.Pool
	chPool *sql.DB
	pgMux  sync.Mutex
	chMux  sync.Mutex
)

func GetPostgresPool(pgURL string) (*pgxpool.Pool, error) {
	pgMux.Lock()
	defer pgMux.Unlock()

	if pgPool != nil {
		return pgPool, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, err
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	pgPool = pool
	return pgPool, nil
}

func GetClickHousePool(chURL string) (*sql.DB, error) {
	chMux.Lock()
	defer chMux.Unlock()

	if chPool != nil {
		return chPool, nil
	}

	conn, err := ConnectClickHouse(chURL)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(1 * time.Hour)
	conn.SetConnMaxIdleTime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}

	chPool = conn
	return chPool, nil
}

func ClosePostgresPool() {
	pgMux.Lock()
	defer pgMux.Unlock()

	if pgPool != nil {
		pgPool.Close()
		pgPool = nil
	}
}

func CloseClickHousePool() {
	chMux.Lock()
	defer chMux.Unlock()

	if chPool != nil {
		chPool.Close()
		chPool = nil
	}
}

func CloseAllPools() {
	ClosePostgresPool()
	CloseClickHousePool()
}

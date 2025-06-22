package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/pixperk/chug/internal/config"
	"github.com/pixperk/chug/internal/db"
	"github.com/pixperk/chug/internal/etl"
	"github.com/pixperk/chug/internal/logx"
	"github.com/pixperk/chug/internal/poller"
	"go.uber.org/zap"
)

func startPolling(ctx context.Context, cfg *config.Config, lastSeen string) error {
	logx.Logger.Info("🔄 Starting change data polling",
		zap.String("table", cfg.Table),
		zap.String("delta_column", cfg.Polling.DeltaCol),
		zap.Int("interval_seconds", cfg.Polling.Interval),
	)

	// Connect to PostgreSQL for polling
	pgConn, err := db.ConnectPostgres(cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL for polling: %w", err)
	}
	defer pgConn.Close(ctx)

	// Define how to handle new data
	processNewData := func(data *etl.TableData) error {
		logx.Logger.Info("📥 Processing new data batch",
			zap.Int("rows", len(data.Rows)),
			zap.String("table", cfg.Table),
		)

		// Insert the new rows
		return etl.InsertRows(cfg.ClickHouseURL, cfg.Table, etl.GetColumnNames(data.Columns), data.Rows, cfg.BatchSize)
	}

	pollConfig := poller.PollConfig{
		Table:     cfg.Table,
		DeltaCol:  cfg.Polling.DeltaCol,
		Interval:  time.Duration(cfg.Polling.Interval) * time.Second,
		Limit:     &cfg.Limit,
		StartFrom: lastSeen,
		OnData:    processNewData,
	}

	p := poller.NewPoller(pgConn, pollConfig)

	return p.Start(ctx)
}

func determineLastSeen(td *etl.TableData, deltaCol string) (string, error) {
	if len(td.Rows) == 0 {
		return "", nil
	}

	deltaColIndex := -1
	for i, col := range td.Columns {
		if col.Name == deltaCol {
			deltaColIndex = i
			break
		}
	}

	if deltaColIndex == -1 {
		return "", fmt.Errorf("delta column %s not found in table", deltaCol)
	}

	lastRow := td.Rows[len(td.Rows)-1]

	switch v := lastRow[deltaColIndex].(type) {
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	case string:
		return v, nil
	case int, int64, int32, int16, int8:
		return fmt.Sprintf("%d", v), nil
	case float64, float32:
		return fmt.Sprintf("%f", v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

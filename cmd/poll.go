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
	"github.com/pixperk/chug/internal/ui"
	"go.uber.org/zap"
)

func startPolling(ctx context.Context, cfg *config.Config, lastSeen string) error {
	log := logx.StyledLog
	log.Highlight("Starting change data polling")

	startFrom := lastSeen
	if startFrom == "" {
		startFrom = "beginning"
	}

	ui.PrintBox("Polling Configuration",
		"Table: "+cfg.Table+"\n"+
			"Delta Column: "+cfg.Polling.DeltaCol+"\n"+
			"Interval: "+fmt.Sprintf("%d seconds", cfg.Polling.Interval)+"\n"+
			"Starting From: "+startFrom)

	// Connect to PostgreSQL pool for polling
	pgConn, err := db.GetPostgresPool(cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL for polling: %w", err)
	}

	// Ensure index exists on delta column for fast polling
	log.Info("Checking/creating index on delta column...")
	if err := etl.EnsureDeltaColumnIndex(context.Background(), pgConn, cfg.Table, cfg.Polling.DeltaCol); err != nil {
		log.Warn("Could not create index on delta column (continuing anyway)", zap.Error(err))
	} else {
		log.Success("Index ready on delta column", zap.String("column", cfg.Polling.DeltaCol))
	}

	// Define how to handle new data
	processNewData := func(data *etl.TableData) error {
		if len(data.Rows) > 0 {
			log.Info(fmt.Sprintf("Processing new data batch: %d rows", len(data.Rows)),
				zap.Int("rows", len(data.Rows)),
				zap.String("table", cfg.Table),
			)
		} else {
			log.Info("No new data found in this polling cycle")
		}

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
	log := logx.StyledLog

	if len(td.Rows) == 0 {
		log.Info("No initial rows to determine last seen value")
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
	var lastSeenValue string

	switch v := lastRow[deltaColIndex].(type) {
	case time.Time:
		// Format as PostgreSQL-compatible timestamp
		lastSeenValue = v.Format("2006-01-02 15:04:05.999999")
	case string:
		lastSeenValue = v
	case int, int64, int32, int16, int8:
		lastSeenValue = fmt.Sprintf("%d", v)
	case float64, float32:
		lastSeenValue = fmt.Sprintf("%f", v)
	default:
		// Fallback: try to convert to time.Time first
		if t, ok := v.(time.Time); ok {
			lastSeenValue = t.Format("2006-01-02 15:04:05.999999")
		} else {
			lastSeenValue = fmt.Sprintf("%v", v)
		}
	}

	log.Info("Determined last seen value for delta tracking",
		zap.String("column", deltaCol),
		zap.String("value", lastSeenValue))

	return lastSeenValue, nil
}

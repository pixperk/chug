package etl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pixperk/chug/internal/config"
)

// TableResult represents the result of ingesting a single table
type TableResult struct {
	TableName string        `json:"name"`
	Success   bool          `json:"success"`
	Error     string        `json:"error,omitempty"`
	RowCount  int64         `json:"rows"`
	Duration  time.Duration `json:"duration"`
}

// IngestOptions contains optional callbacks for logging/monitoring
type IngestOptions struct {
	OnTableStart    func(tableName string)
	OnExtractStart  func(tableName string, columnCount int)
	OnInsertStart   func(tableName string)
	OnProgress      func(tableName string, currentRows int64, totalRows int64, percentage float64, phase string)
	OnTableComplete func(tableName string, rowCount int64, duration time.Duration)
	OnTableError    func(tableName string, err error)
	StartPolling    func(ctx context.Context, tableConfig config.ResolvedTableConfig)
}

// IngestSingleTable ingests a single table from PostgreSQL to ClickHouse
func IngestSingleTable(
	ctx context.Context,
	pgConn *pgxpool.Pool,
	chURL string,
	tableConfig config.ResolvedTableConfig,
	opts *IngestOptions,
) TableResult {
	startTime := time.Now()
	result := TableResult{
		TableName: tableConfig.Name,
		Success:   false,
	}

	if opts != nil && opts.OnTableStart != nil {
		opts.OnTableStart(tableConfig.Name)
	}

	// Extract data from PostgreSQL
	stream, err := ExtractTableDataStreaming(ctx, pgConn, tableConfig.Name, &tableConfig.Limit)
	if err != nil {
		errMsg := fmt.Sprintf("extraction failed: %v", err)
		result.Error = errMsg
		if opts != nil && opts.OnTableError != nil {
			opts.OnTableError(tableConfig.Name, fmt.Errorf("%s", errMsg))
		}
		return result
	}

	if opts != nil && opts.OnExtractStart != nil {
		opts.OnExtractStart(tableConfig.Name, len(stream.Columns))
	}

	// Query for primary key columns if CDC is enabled
	var pkCols []string
	if tableConfig.Polling.Enabled {
		pkCols, _ = GetPrimaryKeyColumns(ctx, pgConn, tableConfig.Name)
	}

	// Build DDL and create table in ClickHouse
	ddl, err := BuildDDLQuery(tableConfig.Name, stream.Columns, tableConfig.Polling.Enabled, tableConfig.Polling.DeltaCol, pkCols)
	if err != nil {
		errMsg := fmt.Sprintf("DDL generation failed: %v", err)
		result.Error = errMsg
		if opts != nil && opts.OnTableError != nil {
			opts.OnTableError(tableConfig.Name, fmt.Errorf("%s", errMsg))
		}
		return result
	}

	if err := CreateTable(chURL, ddl); err != nil {
		errMsg := fmt.Sprintf("table creation failed: %v", err)
		result.Error = errMsg
		if opts != nil && opts.OnTableError != nil {
			opts.OnTableError(tableConfig.Name, fmt.Errorf("%s", errMsg))
		}
		return result
	}

	if opts != nil && opts.OnInsertStart != nil {
		opts.OnInsertStart(tableConfig.Name)
	}

	// Stream rows to ClickHouse
	var rowCount atomic.Int64
	rowChan := make(chan []any, 100)

	// Calculate total rows for percentage
	var totalRows int64
	if tableConfig.Limit > 0 {
		totalRows = int64(tableConfig.Limit)
	}

	// Progress reporting ticker
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()

	// Goroutine for progress updates
	go func() {
		for range progressTicker.C {
			current := rowCount.Load()
			if opts != nil && opts.OnProgress != nil && current > 0 {
				var percentage float64
				if totalRows > 0 {
					percentage = float64(current) / float64(totalRows) * 100
					if percentage > 100 {
						percentage = 100
					}
				}
				opts.OnProgress(tableConfig.Name, current, totalRows, percentage, "inserting")
			}
		}
	}()

	go func() {
		for row := range stream.RowChan {
			rowChan <- row
			rowCount.Add(1)
		}
		close(rowChan)
	}()

	if err := InsertRowsStreaming(ctx, chURL, tableConfig.Name, GetColumnNames(stream.Columns), rowChan, tableConfig.BatchSize); err != nil {
		errMsg := fmt.Sprintf("insertion failed: %v", err)
		result.Error = errMsg
		if opts != nil && opts.OnTableError != nil {
			opts.OnTableError(tableConfig.Name, fmt.Errorf("%s", errMsg))
		}
		return result
	}

	// Check for extraction errors
	select {
	case err := <-stream.ErrChan:
		if err != nil {
			errMsg := fmt.Sprintf("extraction error: %v", err)
			result.Error = errMsg
			if opts != nil && opts.OnTableError != nil {
				opts.OnTableError(tableConfig.Name, fmt.Errorf("%s", errMsg))
			}
			return result
		}
	default:
	}

	result.Success = true
	result.RowCount = rowCount.Load()
	result.Duration = time.Since(startTime)

	if opts != nil && opts.OnTableComplete != nil {
		opts.OnTableComplete(tableConfig.Name, result.RowCount, result.Duration)
	}

	// Start polling if enabled
	if tableConfig.Polling.Enabled && opts != nil && opts.StartPolling != nil {
		go opts.StartPolling(ctx, tableConfig)
	}

	return result
}

// IngestMultipleTables ingests multiple tables in parallel
func IngestMultipleTables(
	ctx context.Context,
	cfg *config.Config,
	pgConn *pgxpool.Pool,
	opts *IngestOptions,
) []TableResult {
	tableConfigs := cfg.GetEffectiveTableConfigs()
	resultChan := make(chan TableResult, len(tableConfigs))
	var wg sync.WaitGroup

	for _, tc := range tableConfigs {
		wg.Add(1)
		go func(tableConfig config.TableConfig) {
			defer wg.Done()
			resolved := cfg.ResolveTableConfig(tableConfig)
			result := IngestSingleTable(ctx, pgConn, cfg.ClickHouseURL, resolved, opts)
			resultChan <- result
		}(tc)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var results []TableResult
	for result := range resultChan {
		results = append(results, result)
	}

	return results
}

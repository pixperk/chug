package poller

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pixperk/chug/internal/etl"
	"github.com/pixperk/chug/internal/logx"
	"go.uber.org/zap"
)

type PollConfig struct {
	Table     string
	DeltaCol  string
	Interval  time.Duration
	Limit     *int
	StartFrom string
	OnData    func(data *etl.TableData) error
}

type Poller struct {
	conn   *pgx.Conn
	config PollConfig
}

func NewPoller(conn *pgx.Conn, config PollConfig) *Poller {
	return &Poller{
		conn:   conn,
		config: config,
	}
}

func (p *Poller) Start(ctx context.Context) error {
	lastSeen := p.config.StartFrom

	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	logx.Logger.Info("Starting poller",
		zap.String("table", p.config.Table),
		zap.Duration("interval", p.config.Interval))

	for {
		select {
		case <-ctx.Done():
			logx.Logger.Info("Stopping (context cancelled)")
			return ctx.Err()

		case <-ticker.C:
			logx.Logger.Info("Polling for new data",
				zap.String("table", p.config.Table),
				zap.String("last_seen", lastSeen))
			data, err := etl.ExtractTableDataSince(ctx, p.conn, p.config.Table, p.config.DeltaCol, lastSeen, p.config.Limit)
			if err != nil {
				logx.Logger.Error("Failed to extract table data",
					zap.String("table", p.config.Table),
					zap.Error(err))
				continue
			}
			if len(data.Rows) == 0 {
				logx.Logger.Info("No new data found",
					zap.String("table", p.config.Table),
					zap.String("last_seen", lastSeen))
				continue
			} //update last seen value
			lastRow := data.Rows[len(data.Rows)-1]
			for i, col := range data.Columns {
				if col.Name == p.config.DeltaCol {
					switch v := lastRow[i].(type) {
					case time.Time:
						lastSeen = v.Format(time.RFC3339Nano)
					case string:
						lastSeen = v
					case int, int64, int32, int16, int8:
						lastSeen = fmt.Sprintf("%d", v)
					case float64, float32:
						lastSeen = fmt.Sprintf("%f", v)
					default:
						lastSeen = fmt.Sprintf("%v", v)
					}
					break
				}
			}

			logx.Logger.Info("New data extracted",
				zap.String("table", p.config.Table),
				zap.Int("rows", len(data.Rows)),
				zap.String("last_seen", lastSeen))

			if err := p.config.OnData(data); err != nil {
				logx.Logger.Error("Failed to process extracted data",
					zap.String("table", p.config.Table),
					zap.Error(err))
				continue
			}
		}
	}
}

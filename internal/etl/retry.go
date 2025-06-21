package etl

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/pixperk/chug/internal/logx"
	"go.uber.org/zap"
)

type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      bool
}

func Retry(ctx context.Context, config RetryConfig, operation func() error) error {
	var attempt int
	for {
		err := operation()
		if err == nil {
			return nil
		}
		attempt++
		if attempt >= config.MaxAttempts {
			return errors.New("max retry attempts reached: " + err.Error())
		}

		//Exponential backoff with jitter
		backoff := min(config.BaseDelay*time.Duration(math.Pow(2, float64(attempt))), config.MaxDelay)
		if config.Jitter {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			backoff += jitter
		}

		logx.Logger.Warn("Operation failed, retrying",
			zap.Int("attempt", attempt),
			zap.Error(err),
			zap.Duration("backoff", backoff),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			continue
		}

	}
}

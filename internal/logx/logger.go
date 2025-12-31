package logx

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

func InitLogger() {
	InitLoggerWithLevel(false)
}

func InitLoggerWithLevel(verbose bool) {
	var err error

	if verbose {
		// Development mode: detailed logs, stack traces, console output
		Logger, err = zap.NewDevelopment()
	} else {
		// Production mode: JSON logs, less verbose
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel) // Only warnings and errors
		Logger, err = config.Build()
	}

	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}

	InitStyledLogger()
}

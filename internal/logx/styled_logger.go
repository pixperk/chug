package logx

import (
	"github.com/pixperk/chug/internal/ui"
	"go.uber.org/zap"
)

// StyledLogger is a wrapper around zap.Logger that provides styled output
type StyledLogger struct {
	logger *zap.Logger
}

// NewStyledLogger creates a new StyledLogger
func NewStyledLogger() *StyledLogger {
	return &StyledLogger{
		logger: Logger,
	}
}

// Info logs a message with INFO level and applies styled output
func (s *StyledLogger) Info(msg string, fields ...zap.Field) {
	s.logger.Info(msg, fields...)
	ui.PrintInfo(msg)
}

// Success logs a message with INFO level and applies success styling
func (s *StyledLogger) Success(msg string, fields ...zap.Field) {
	s.logger.Info(msg, fields...)
	ui.PrintSuccess(msg)
}

// Error logs a message with ERROR level and applies error styling
func (s *StyledLogger) Error(msg string, fields ...zap.Field) {
	s.logger.Error(msg, fields...)
	ui.PrintError(msg)
}

// Warn logs a message with WARN level and applies warning styling
func (s *StyledLogger) Warn(msg string, fields ...zap.Field) {
	s.logger.Warn(msg, fields...)
	ui.PrintWarning(msg)
}

// Fatal logs a message with FATAL level, applies error styling, and exits
func (s *StyledLogger) Fatal(msg string, fields ...zap.Field) {
	s.logger.Fatal(msg, fields...)
	// This will never be reached due to os.Exit in Fatal,
	// but we'll keep it for completeness
	ui.ExitWithError(msg)
}

// Debug logs a message with DEBUG level without visual styling
func (s *StyledLogger) Debug(msg string, fields ...zap.Field) {
	s.logger.Debug(msg, fields...)
	// We don't apply styling to debug messages as they're meant for development
}

// Highlight logs a message with INFO level and applies highlight styling
func (s *StyledLogger) Highlight(msg string, fields ...zap.Field) {
	s.logger.Info(msg, fields...)
	ui.PrintHighlight(msg)
}

// With returns a new StyledLogger with the given fields added to it
func (s *StyledLogger) With(fields ...zap.Field) *StyledLogger {
	return &StyledLogger{
		logger: s.logger.With(fields...),
	}
}

// GetZapLogger returns the underlying zap.Logger
func (s *StyledLogger) GetZapLogger() *zap.Logger {
	return s.logger
}

// Initialize a global instance
var StyledLog *StyledLogger

// InitStyledLogger initializes the global StyledLogger
func InitStyledLogger() {
	StyledLog = NewStyledLogger()
}

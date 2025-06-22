package etl

import (
	"regexp"
	"strings"
)

// QuoteIdentifier properly quotes an identifier (table or column name) for ClickHouse
func QuoteIdentifier(identifier string) string {
	// Escape internal double quotes by doubling them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	// Wrap in double quotes (ANSI-style quoting)
	return `"` + escaped + `"`
}

// IsValidIdentifier checks if the identifier contains only valid characters
func IsValidIdentifier(identifier string) bool {
	// This regex pattern allows alphanumeric characters, underscores, and dots (for schema.table format)
	pattern := regexp.MustCompile(`^[a-zA-Z0-9_\.]+$`)
	return pattern.MatchString(identifier)
}

package etl

import (
	"regexp"
	"strings"
)

// QuoteIdentifier properly quotes an identifier (table or column name) to prevent SQL injection
func QuoteIdentifier(identifier string) string {
	// Remove any existing backticks and replace with double backticks (escaping)
	escaped := strings.ReplaceAll(identifier, "`", "``")
	// Wrap the identifier in backticks
	return "`" + escaped + "`"
}

// IsValidIdentifier checks if the identifier contains only valid characters
func IsValidIdentifier(identifier string) bool {
	// This regex pattern allows alphanumeric characters, underscores, and dots (for schema.table format)
	pattern := regexp.MustCompile(`^[a-zA-Z0-9_\.]+$`)
	return pattern.MatchString(identifier)
}

package etl

import "strings"

func join(arr []string, sep string) string {
	return strings.Join(arr, sep)
}

func buildValuesPlaceholders(rowCount, colCount int) string {
	var builder strings.Builder
	// Pre-allocate approximate capacity: "(?,?,...)" * rowCount
	builder.Grow(rowCount * (colCount*2 + 3))

	for i := 0; i < rowCount; i++ {
		builder.WriteString("(")
		for j := 0; j < colCount; j++ {
			builder.WriteString("?")
			if j < colCount-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(")")
		if i < rowCount-1 {
			builder.WriteString(", ")
		}
	}
	return builder.String()
}

func flatten(matrix [][]any) []any {
	var out []any
	for _, row := range matrix {
		out = append(out, row...)
	}
	return out
}

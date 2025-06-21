package etl

func join(arr []string, sep string) string {
	if len(arr) == 0 {
		return ""
	}
	if len(arr) == 1 {
		return arr[0]
	}
	return arr[0] + sep + join(arr[1:], sep)
}

func buildValuesPlaceholders(rowCount, colCount int) string {
	s := ""
	for i := range rowCount {
		s += "("
		for j := range colCount {
			s += "?"
			if j < colCount-1 {
				s += ", "
			}

		}
		s += ")"
		if i < rowCount-1 {
			s += ", "
		}
	}
	return s
}

func flatten(matrix [][]any) []any {
	var out []any
	for _, row := range matrix {
		out = append(out, row...)
	}
	return out
}

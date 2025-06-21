package etl

import (
	"fmt"
	"strings"
)

func MapColumnTypes(cols []Column) ([]string, error) {
	var mapped []string
	for _, col := range cols {
		chType, ok := pgToCHType[col.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported column type %s for column %s", col.Type, col.Name)
		}
		mapped = append(mapped, fmt.Sprintf("`%s` %s", col.Name, chType))
	}
	return mapped, nil
}

func BuildDDLQuery(table string, cols []Column) (string, error) {
	mappedCols, err := MapColumnTypes(cols)
	if err != nil {
		return "", err
	}
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple();", table, strings.Join(mappedCols, ", "))
	if len(mappedCols) == 0 {
		return "", fmt.Errorf("no valid columns to create table %s", table)
	}
	return ddl, nil
}

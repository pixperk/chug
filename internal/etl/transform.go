package etl

import (
	"fmt"
	"log"
	"strings"
)

func MapColumnTypes(cols []Column) ([]string, error) {
	var mapped []string
	for _, col := range cols {
		chType, ok := pgToCHType[col.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported column type %s for column %s", col.Type, col.Name)
		}
		mapped = append(mapped, fmt.Sprintf("%s %s", col.Name, chType))
	}
	return mapped, nil
}

func BuildDDLQuery(table string, cols []Column, cdcEnabled bool, versionCol string, pkCols []string) (string, error) {
	mappedCols, err := MapColumnTypes(cols)
	if err != nil {
		return "", err
	}
	if len(mappedCols) == 0 {
		return "", fmt.Errorf("no valid columns to create table %s", table)
	}

	var orderBy string
	var finalCols []string
	var engine string

	if cdcEnabled && versionCol != "" {
		// Use primary key columns for hash, fallback to all columns
		hashCols := pkCols
		if len(hashCols) == 0 {
			hashCols = make([]string, len(cols))
			for i, col := range cols {
				hashCols[i] = col.Name
			}
		}

		// Add materialized hash column for deduplication
		hashCol := fmt.Sprintf("_dedup_key UInt64 MATERIALIZED cityHash64(tuple(%s))", strings.Join(hashCols, ", "))
		finalCols = append(mappedCols, hashCol)
		orderBy = "_dedup_key"
		engine = fmt.Sprintf("ReplacingMergeTree(%s)", versionCol)
	} else {
		finalCols = mappedCols
		orderBy = "tuple()"
		engine = "MergeTree()"
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = %s ORDER BY %s;",
		QuoteIdentifier(table),
		strings.Join(finalCols, ", "),
		engine,
		orderBy,
	)

	log.Printf("Generated DDL: %s\n", ddl)
	return ddl, nil
}

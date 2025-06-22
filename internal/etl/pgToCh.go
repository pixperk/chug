package etl

var pgToCHType = map[string]string{
	"integer":                     "Int32",
	"bigint":                      "Int64",
	"smallint":                    "Int16",
	"serial":                      "Int32",
	"bigserial":                   "Int64",
	"boolean":                     "Bool",
	"text":                        "String",
	"varchar":                     "String",
	"character varying":           "String",
	"char":                        "String",
	"date":                        "Date",
	"timestamp":                   "DateTime",
	"timestamp without time zone": "DateTime",
	"timestamp with time zone":    "DateTime",
	"numeric":                     "Float64",
	"decimal":                     "Float64",
	"double precision":            "Float64",
	"real":                        "Float32",
	"json":                        "String", // or JSON object if ClickHouse supports it in future
	"jsonb":                       "String",
	"uuid":                        "UUID",
	"bytea":                       "UUID", // pgx v5 uses bytea for uuid
	"inet":                        "String",
	"USER-DEFINED":                "String", // fallback
}

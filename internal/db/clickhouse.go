package db

import (
	"context"
	"net/url"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestClickHouse(chURL string) error {
	var addr string
	var username, password, database string

	// Support both http://localhost:9000 and plain localhost:9000
	if strings.HasPrefix(chURL, "http://") || strings.HasPrefix(chURL, "tcp://") {
		u, err := url.Parse(chURL)
		if err != nil {
			return err
		}
		addr = u.Host
		if u.User != nil {
			username = u.User.Username()
			password, _ = u.User.Password()
		}
		if db := strings.Trim(u.Path, "/"); db != "" {
			database = db
		}
	} else {
		addr = chURL
	}

	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: ifEmpty(database, "default"),
			Username: ifEmpty(username, "default"),
			Password: password,
		},
		Settings: clickhouse.Settings{
			"send_logs_level": "trace", // for debug visibility
		},
	})

	return conn.PingContext(context.Background())
}

func ifEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

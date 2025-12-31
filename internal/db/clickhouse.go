package db

import (
	"crypto/tls"
	"database/sql"
	"net/url"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ConnectClickHouse(chURL string) (*sql.DB, error) {

	var addr string
	var username, password, database string
	var useTLS bool

	// Support http://, https://, tcp://, and plain host:port
	if strings.HasPrefix(chURL, "http://") || strings.HasPrefix(chURL, "https://") || strings.HasPrefix(chURL, "tcp://") {
		u, err := url.Parse(chURL)
		if err != nil {
			return nil, err
		}
		addr = u.Host
		if u.User != nil {
			username = u.User.Username()
			password, _ = u.User.Password()
		}
		if db := strings.Trim(u.Path, "/"); db != "" {
			database = db
		}
		useTLS = strings.HasPrefix(chURL, "https://")
	} else {
		addr = chURL
	}

	opts := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: ifEmpty(database, "default"),
			Username: ifEmpty(username, "default"),
			Password: password,
		},
		Settings: clickhouse.Settings{
			"send_logs_level": "trace",
		},
	}

	if useTLS {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	return clickhouse.OpenDB(opts), nil

}

func ifEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

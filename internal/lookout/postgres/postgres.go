package postgres

import (
	"database/sql"
	"strings"

	_ "github.com/lib/pq"

	"github.com/G-Research/armada/internal/lookout/configuration"
)

func Open(config configuration.PostgresConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", createConnectionString(config.Connection))
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	return db, nil
}

func createConnectionString(values map[string]string) string {
	// https://www.postgresql.org/docs/10/libpq-connect.html#id-1.7.3.8.3.5
	result := ""
	replacer := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	for k, v := range values {
		result += k + "='" + replacer.Replace(v) + "'"
	}
	return result
}

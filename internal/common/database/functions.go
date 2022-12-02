package database

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/internal/armada/configuration"
)

func CreateConnectionString(values map[string]string) string {
	// https://www.postgresql.org/docs/10/libpq-connect.html#id-1.7.3.8.3.5
	result := ""
	replacer := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	for k, v := range values {
		result += k + "='" + replacer.Replace(v) + "'"
	}
	return result
}

func OpenPgxPool(config configuration.PostgresConfig) (*pgxpool.Pool, error) {
	db, err := pgxpool.Connect(context.Background(), CreateConnectionString(config.Connection))
	if err != nil {
		return nil, err
	}
	err = db.Ping(context.Background())
	return db, err
}

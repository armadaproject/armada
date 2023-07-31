package postgres

import (
	"context"
	"strings"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/database/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresConnection implements database.DatabaseConnection
type PostgresConnection struct {
	Config configuration.DatabaseConfig
}

func (c PostgresConnection) GetConfig() configuration.DatabaseConfig {
	return c.Config
}

func (c PostgresConnection) Open() (types.DatabaseConn, error) {
	db, err := pgx.Connect(context.Background(), createConnectionString(c.Config.Connection))
	if err != nil {
		return nil, err
	}

	return PostgresConnAdapter{Conn: db}, err
}

func OpenPool(config configuration.DatabaseConfig) (types.DatabasePool, error) {
	db, err := pgxpool.New(context.Background(), createConnectionString(config.Connection))
	if err != nil {
		return nil, err
	}
	err = db.Ping(context.Background())
	return PostgresPoolAdapter{Pool: db}, err
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

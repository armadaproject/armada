package postgres

import (
	"database/sql"
	"github.com/G-Research/armada/internal/common/database"
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/G-Research/armada/internal/armada/configuration"
)

func Open(config configuration.PostgresConfig) (*sql.DB, error) {
	db, err := sql.Open("pgx", database.CreateConnectionString(config.Connection))
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	return db, nil
}

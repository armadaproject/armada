package postgres

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/database"
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

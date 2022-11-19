package database

import (
	"embed"
	_ "embed"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v4/pgxpool"
)

//go:embed migrations/*.sql
var fs embed.FS

func UpdateDatabase(databaseUrl string) error {
	driver, err := iofs.New(fs, "migrations")
	if err != nil {
		return err
	}
	return database.MigrateDatabase(driver, databaseUrl)
}

func WithTestDb(action func(queries *Queries, db *pgxpool.Pool) error) error {
	driver, err := iofs.New(fs, "migrations")
	if err != nil {
		return err
	}
	return database.WithTestDb2(driver, func(db *pgxpool.Pool) error {
		return action(New(db), db)
	})
}

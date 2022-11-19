package database

import (
	"context"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	foo "github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/pkg/errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/internal/armada/configuration"
)

func MigrateDatabase(sourceInstance source.Driver, databaseUrl string) error {
	p := &foo.Postgres{}
	database, err := p.Open(databaseUrl)
	m, err := migrate.NewWithInstance("iofs", sourceInstance, "db", database)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(m.Up())
}

func UniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
}

func BatchInsert(ctx context.Context, db *pgxpool.Pool, createTmp func(pgx.Tx) error,
	insertTmp func(pgx.Tx) error, copyToDest func(pgx.Tx) error,
) error {
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		// Create a temporary table to hold the staging data
		err := createTmp(tx)
		if err != nil {
			return err
		}

		err = insertTmp(tx)
		if err != nil {
			return err
		}

		err = copyToDest(tx)
		if err != nil {
			return err
		}
		return nil
	})
}

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

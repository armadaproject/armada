package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/internal/armada/configuration"
)

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

func ParseNullStringDefault(nullString sql.NullString) string {
	if !nullString.Valid {
		return ""
	}
	return nullString.String
}

func ParseNullString(nullString sql.NullString) *string {
	if !nullString.Valid {
		return nil
	}
	return &nullString.String
}

func ParseNullTime(nullTime sql.NullTime) *time.Time {
	if !nullTime.Valid {
		return nil
	}
	return &nullTime.Time
}

func ParseNullTimeDefault(nullTime sql.NullTime) time.Time {
	if !nullTime.Valid {
		return time.Now()
	}
	return nullTime.Time
}

func ParseNullInt32(nullInt sql.NullInt32) *int32 {
	if !nullInt.Valid {
		return nil
	}
	return &nullInt.Int32
}

func ParseNullInt16Default(nullInt sql.NullInt16) int16 {
	if !nullInt.Valid {
		return 0
	}
	return nullInt.Int16
}

func ReadInt(rows pgx.Rows) (int, error) {
	defer rows.Close()
	var val int
	for rows.Next() {
		err := rows.Scan(&val)
		if err != nil {
			return -1, err
		}
		return val, nil
	}
	return -1, errors.New("no rows found")
}

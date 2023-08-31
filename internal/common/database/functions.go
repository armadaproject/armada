package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
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

func OpenPgxConn(config configuration.PostgresConfig) (*pgx.Conn, error) {
	db, err := pgx.Connect(context.Background(), CreateConnectionString(config.Connection))
	if err != nil {
		return nil, err
	}
	err = db.Ping(context.Background())
	return db, err
}

func OpenPgxPool(config configuration.PostgresConfig) (*pgxpool.Pool, error) {
	db, err := pgxpool.New(context.Background(), CreateConnectionString(config.Connection))
	if err != nil {
		return nil, err
	}
	err = db.Ping(context.Background())
	return db, err
}

func UniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
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

func ParseNullInt32(nullInt sql.NullInt32) *int32 {
	if !nullInt.Valid {
		return nil
	}
	return &nullInt.Int32
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

package clickhouseingester

import (
	"database/sql"
	"embed"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pkg/errors"
	"github.com/pressly/goose/v3"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

func OpenClickHouse(ctx *armadacontext.Context, addr, database, username, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})

	if err != nil {
		return nil, errors.WithMessagef(err, "could not connect to clickhouse on %s", addr)
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, errors.WithMessagef(err, "failed to ping clickhouse at %s", addr)
	}
	return conn, nil
}

func MigrateDB(ctx *armadacontext.Context, clickhouseDSN string) error {
	db, err := sql.Open("clickhouse", clickhouseDSN)
	if err != nil {
		return errors.WithMessage(err, "error opening connection to clickhouse")
	}
	defer util.CloseResource("Lookout Clickhouse", db)

	goose.SetBaseFS(embeddedMigrations)

	if err := goose.SetDialect("clickhouse"); err != nil {
		return errors.WithMessage(err, "failed to set goose dialect")
	}

	if err := goose.Up(db, "migrations"); !errors.Is(err, goose.ErrNoNextVersion) && err != nil {
		return errors.WithMessage(err, "failed to run lookout clickhouse migrations")
	}

	ctx.Info("Database migrations completed successfully")
	return nil
}

func withTestDb(ctx *armadacontext.Context, f func(db clickhouse.Conn)) error {
	dbName := fmt.Sprintf("test_%s", util.NewULID())

	// Admin connection for DB create/drop
	adminDb, err := OpenClickHouse(ctx, "localhost:9000", "default", "clickhouse", "psw")
	if err != nil {
		return err
	}
	defer util.CloseResource("admin-clickhouse", adminDb)

	// Create test DB
	if err := adminDb.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, dbName)); err != nil {
		return fmt.Errorf("failed to create test database: %w", err)
	}

	// Build DSN for migrations
	dsn := fmt.Sprintf("clickhouse://clickhouse:psw@localhost:9000/%s", dbName)

	// Run migrations on test DB
	if err := MigrateDB(ctx, dsn); err != nil {
		return errors.WithMessage(err, "failed to migrate test database")
	}

	// Connect to test DB
	testDb, err := OpenClickHouse(ctx, "localhost:9000", dbName, "clickhouse", "psw")
	if err != nil {
		return fmt.Errorf("failed to connect to test database: %w", err)
	}
	defer util.CloseResource("test-clickhouse", testDb)

	// Run test
	f(testDb)

	// Drop test DB
	if err := adminDb.Exec(ctx, fmt.Sprintf(`DROP DATABASE %s`, dbName)); err != nil {
		return fmt.Errorf("failed to drop test database: %w", err)
	}

	return nil
}

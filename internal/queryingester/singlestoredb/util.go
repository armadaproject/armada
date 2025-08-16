package singlestoredb

import (
	"database/sql"
	"embed"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql" // SingleStore/MySQL driver
	"github.com/pkg/errors"
	"github.com/pressly/goose/v3"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// OpenSingleStore opens a connection to a SingleStore (MySQL protocol) cluster.
func OpenSingleStore(ctx *armadacontext.Context, addr, database, username, password string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&multiStatements=true",
		username, password, addr, database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not connect to SingleStore on %s", addr)
	}

	// Optional: set connection pool settings
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return nil, errors.WithMessagef(err, "failed to ping SingleStore at %s", addr)
	}
	return db, nil
}

// MigrateDB runs Goose migrations against a SingleStore cluster.
func MigrateDB(ctx *armadacontext.Context, dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.WithMessage(err, "error opening connection to SingleStore")
	}
	defer util.CloseResource("Lookout SingleStore", db)

	goose.SetBaseFS(embeddedMigrations)

	// Use the MySQL dialect for SingleStore
	if err := goose.SetDialect("mysql"); err != nil {
		return errors.WithMessage(err, "failed to set goose dialect")
	}

	if err := goose.Up(db, "migrations"); !errors.Is(err, goose.ErrNoNextVersion) && err != nil {
		return errors.WithMessage(err, "failed to run lookout SingleStore migrations")
	}

	ctx.Info("Database migrations completed successfully")
	return nil
}

func withTestDb(ctx *armadacontext.Context, f func(db *sql.DB)) error {
	dbName := fmt.Sprintf("test_%s", util.NewULID())

	// Admin connection for DB create/drop
	adminDb, err := OpenSingleStore(ctx, "localhost:3306", "memsql", "root", "psw")
	if err != nil {
		return err
	}
	defer util.CloseResource("admin-singlestore", adminDb)

	// Create test DB
	if _, err := adminDb.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s`, dbName)); err != nil {
		return fmt.Errorf("failed to create test database: %w", err)
	}

	// Build DSN for migrations
	dsn := fmt.Sprintf("root:psw@tcp(localhost:3306)/%s?parseTime=true&multiStatements=true", dbName)

	// Run migrations on test DB
	if err := MigrateDB(ctx, dsn); err != nil {
		return errors.WithMessage(err, "failed to migrate test database")
	}

	// Connect to test DB
	testDb, err := OpenSingleStore(ctx, "localhost:3306", dbName, "root", "psw")
	if err != nil {
		return fmt.Errorf("failed to connect to test database: %w", err)
	}
	defer util.CloseResource("test-singlestore", testDb)

	// Run test
	f(testDb)

	// Drop test DB
	if _, err := adminDb.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE %s`, dbName)); err != nil {
		return fmt.Errorf("failed to drop test database: %w", err)
	}

	return nil
}

func ptrVal[T comparable](p *T) T {
	if p != nil {
		return *p
	}
	var zero T
	return zero
}

func ptrBool(p *bool) bool {
	if p != nil {
		return *p
	}
	return false
}

func ptrTime(p *time.Time) time.Time {
	if p != nil {
		return *p
	}
	return time.Time{}
}

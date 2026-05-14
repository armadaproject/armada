package database

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/server/configuration"
)

// MigrationConfig configures optional schema-creation behaviour for the
// database migrator. The zero value preserves the historical behaviour: no
// search_path change, no schema creation.
type MigrationConfig struct {
	// Schema is the schema in which migrations should run. If set, the
	// migrator will SET search_path to this value before applying migrations.
	// If unset, the connection's default search_path is used.
	Schema string

	// CreateSchema, if true, ensures Schema exists before running
	// migrations. Requires Schema to be set.
	CreateSchema bool

	// SchemaCreator optionally configures a separate "bootstrap" connection
	// used only to CREATE SCHEMA and GRANT privileges to the migrator role.
	// If nil and CreateSchema is true, the migrator's own connection is used
	// (single-role pattern).
	SchemaCreator *configuration.PostgresConfig
}

// maxIdentifierLength is the Postgres limit (NAMEDATALEN-1) for unquoted and
// quoted identifiers. Postgres silently truncates beyond this, which can
// cause distinct configured names to collide in the database.
const maxIdentifierLength = 63

func validateIdentifier(name string) error {
	if len(name) == 0 {
		return errors.New("identifier must not be empty")
	}
	if len(name) > maxIdentifierLength {
		return errors.Errorf("identifier %q exceeds maximum length of %d bytes", name, maxIdentifierLength)
	}
	return nil
}

// PrepareSchema prepares the migrator session according to cfg. It optionally
// creates the configured schema (using a separate bootstrap connection if
// SchemaCreator is set) and sets the migrator session's search_path so that
// subsequent unqualified DDL lands in the configured schema.
//
// PrepareSchema must be called before UpdateDatabase. It is idempotent.
//
// migratorDB must be a single connection (not a pool): SET search_path is
// session-local, so any subsequent migration query must run on the same
// connection or the search_path will not be in effect.
func PrepareSchema(ctx *armadacontext.Context, migratorDB *pgx.Conn, cfg MigrationConfig) error {
	if cfg.CreateSchema && cfg.Schema == "" {
		return errors.New("CreateSchema requires Schema to be set")
	}
	if cfg.Schema == "" {
		return nil
	}
	if err := validateIdentifier(cfg.Schema); err != nil {
		return errors.WithMessage(err, "invalid migration schema name")
	}

	if cfg.CreateSchema {
		if err := createSchema(ctx, migratorDB, cfg); err != nil {
			return err
		}
	}

	schemaIdent := pgx.Identifier{cfg.Schema}.Sanitize()
	if _, err := migratorDB.Exec(ctx, fmt.Sprintf(`SET search_path TO %s`, schemaIdent)); err != nil {
		return errors.WithMessagef(err, "failed to set search_path to %q", cfg.Schema)
	}
	return nil
}

func createSchema(ctx *armadacontext.Context, migratorDB *pgx.Conn, cfg MigrationConfig) error {
	schemaIdent := pgx.Identifier{cfg.Schema}.Sanitize()

	if cfg.SchemaCreator == nil {
		if _, err := migratorDB.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdent)); err != nil {
			return errors.WithMessagef(err, "failed to create schema %q", cfg.Schema)
		}
		return nil
	}

	migratorRole, err := currentUser(ctx, migratorDB)
	if err != nil {
		return errors.WithMessage(err, "failed to determine migrator role")
	}
	if err := validateIdentifier(migratorRole); err != nil {
		return errors.WithMessage(err, "unexpected migrator role name")
	}
	roleIdent := pgx.Identifier{migratorRole}.Sanitize()

	bootstrapDB, err := OpenPgxConn(*cfg.SchemaCreator)
	if err != nil {
		return errors.WithMessage(err, "failed to connect as schema-creator role")
	}
	defer func() { _ = bootstrapDB.Close(ctx) }()

	if _, err := bootstrapDB.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdent)); err != nil {
		return errors.WithMessagef(err, "failed to create schema %q as schema-creator role", cfg.Schema)
	}
	if _, err := bootstrapDB.Exec(ctx, fmt.Sprintf(`GRANT USAGE, CREATE ON SCHEMA %s TO %s`, schemaIdent, roleIdent)); err != nil {
		return errors.WithMessagef(err, "failed to grant privileges on schema %q to %q", cfg.Schema, migratorRole)
	}
	return nil
}

func currentUser(ctx *armadacontext.Context, db *pgx.Conn) (string, error) {
	var user string
	if err := db.QueryRow(ctx, `SELECT current_user`).Scan(&user); err != nil {
		return "", err
	}
	return user, nil
}

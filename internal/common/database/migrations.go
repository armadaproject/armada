package database

import (
	"cmp"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// Migration represents a single, versioned database migration. It has a
// default variant and optionally other named variants, each made up of one
// or more SQL statements that are executed separately, in order.
type Migration struct {
	id       int
	name     string
	variants map[string][]string
}

func NewMigration(id int, name string, sql string) Migration {
	return Migration{
		id:       id,
		name:     name,
		variants: map[string][]string{"": {sql}},
	}
}

func (m Migration) ID() int {
	return m.id
}

func (m Migration) Name() string {
	return m.name
}

// VariantSelector returns the name of the migration variant to apply to db,
// or "" for the default variant. UpdateDatabaseWithVariants calls it before
// applying each migration.
type VariantSelector func(ctx *armadacontext.Context, db Querier) (string, error)

func UpdateDatabase(ctx *armadacontext.Context, db Querier, migrations []Migration) error {
	return UpdateDatabaseWithVariants(ctx, db, migrations, nil)
}

// UpdateDatabaseWithVariants applies each migration with an ID greater than
// the current database version, selecting the variant to apply with
// selectVariant. A migration that does not have the selected variant falls
// back to its default variant; if it has neither, it is recorded as applied
// without executing anything. A nil selectVariant always selects the default.
func UpdateDatabaseWithVariants(ctx *armadacontext.Context, db Querier, migrations []Migration, selectVariant VariantSelector) error {
	ctx.Info("Preparing to apply postgres migrations.")
	version, err := readVersion(ctx, db)
	if err != nil {
		return err
	}
	ctx.Infof("Current version: %d", version)

	originalVersion := version
	for _, m := range migrations {
		if m.id > version {
			variant := ""
			if selectVariant != nil {
				variant, err = selectVariant(ctx, db)
				if err != nil {
					return errors.Wrapf(err, "select variant for %s", m.name)
				}
			}
			statements, ok := m.variants[variant]
			if !ok {
				variant = ""
				statements = m.variants[variant]
			}
			if variant == "" {
				ctx.Infof("Applying %s", m.name)
			} else {
				ctx.Infof("Applying %s (%s variant)", m.name, variant)
			}
			for _, statement := range statements {
				if _, err := db.Exec(ctx, statement); err != nil {
					return err
				}
			}

			version = m.id
			err = setVersion(ctx, db, version)
			if err != nil {
				return err
			}
		} else {
			ctx.Infof("Not applying %s as migration id %d is <= postgres version %d", m.name, m.id, version)
		}
	}

	if version == originalVersion {
		ctx.Info("Postgres was already the up-to-date")
	} else {
		ctx.Infof("Postgres updates from version %d to %d", originalVersion, version)
	}
	return nil
}

func readVersion(ctx *armadacontext.Context, db Querier) (int, error) {
	_, err := db.Exec(ctx,
		`CREATE SEQUENCE IF NOT EXISTS database_version START WITH 0 MINVALUE 0;`)
	if err != nil {
		return 0, err
	}

	result, err := db.Query(ctx,
		`SELECT last_value FROM database_version`)
	if err != nil {
		return 0, err
	}
	defer result.Close()
	var version int
	result.Next()
	err = result.Scan(&version)

	return version, err
}

func setVersion(ctx *armadacontext.Context, db Querier, version int) error {
	_, err := db.Exec(ctx, `SELECT setval('database_version', $1)`, version)
	return err
}

// ReadMigrations reads the migrations in basePath. Each entry is named
// "<id>_<name>" for the default variant or "<id>_<name>.<variant>" for one
// of the given variants, and is either a ".sql" file, executed as a single
// statement, or a directory of ".sql" files, executed separately in name
// order. Executing statements separately lets a migration run statements
// that cannot run inside a transaction, such as CREATE INDEX CONCURRENTLY.
func ReadMigrations(fsys fs.FS, basePath string, variants ...string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, basePath)
	if err != nil {
		return nil, err
	}

	migrationsByName := map[string]*Migration{}
	for _, entry := range entries {
		stem := entry.Name()
		if !entry.IsDir() {
			if !strings.HasSuffix(stem, ".sql") {
				continue
			}
			stem = strings.TrimSuffix(stem, ".sql")
		}
		name, variant := splitVariant(stem, variants)

		id, err := strconv.Atoi(strings.Split(name, "_")[0])
		if err != nil {
			return nil, err
		}

		statements, err := readStatements(fsys, path.Join(basePath, entry.Name()), entry.IsDir())
		if err != nil {
			return nil, err
		}

		m, ok := migrationsByName[name]
		if !ok {
			m = &Migration{id: id, name: name, variants: map[string][]string{}}
			migrationsByName[name] = m
		}
		if _, ok := m.variants[variant]; ok {
			return nil, fmt.Errorf("migration %s has more than one %q variant", name, variant)
		}
		m.variants[variant] = statements
	}

	migrations := make([]Migration, 0, len(migrationsByName))
	for _, m := range migrationsByName {
		migrations = append(migrations, *m)
	}
	slices.SortFunc(migrations, func(a, b Migration) int {
		return cmp.Or(cmp.Compare(a.id, b.id), cmp.Compare(a.name, b.name))
	})
	return migrations, nil
}

func splitVariant(stem string, variants []string) (string, string) {
	for _, variant := range variants {
		if name, ok := strings.CutSuffix(stem, "."+variant); ok {
			return name, variant
		}
	}
	return stem, ""
}

func readStatements(fsys fs.FS, entryPath string, isDir bool) ([]string, error) {
	if !isDir {
		bytes, err := fs.ReadFile(fsys, entryPath)
		if err != nil {
			return nil, err
		}
		return []string{string(bytes)}, nil
	}

	files, err := fs.ReadDir(fsys, entryPath)
	if err != nil {
		return nil, err
	}

	var statements []string
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".sql") {
			continue
		}
		bytes, err := fs.ReadFile(fsys, path.Join(entryPath, f.Name()))
		if err != nil {
			return nil, err
		}
		statements = append(statements, string(bytes))
	}
	if len(statements) == 0 {
		return nil, fmt.Errorf("migration directory %s contains no .sql files", entryPath)
	}
	return statements, nil
}

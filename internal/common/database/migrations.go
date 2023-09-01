package database

import (
	"bytes"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	stakikfs "github.com/rakyll/statik/fs"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// Migration represents a single, versioned database migration script
type Migration struct {
	id   int
	name string
	sql  string
}

func NewMigration(id int, name string, sql string) Migration {
	return Migration{
		id:   id,
		name: name,
		sql:  sql,
	}
}

func UpdateDatabase(ctx *armadacontext.ArmadaContext, db Querier, migrations []Migration) error {
	log.Info("Updating postgres...")
	version, err := readVersion(ctx, db)
	if err != nil {
		return err
	}
	log.Infof("Current version %v", version)

	for _, m := range migrations {
		if m.id > version {
			log.Debugf("Executing %s", m.name)
			_, err := db.Exec(ctx, m.sql)
			if err != nil {
				return err
			}

			version = m.id
			err = setVersion(ctx, db, version)
			if err != nil {
				return err
			}
		}
	}
	log.Info("Database updated.")
	return nil
}

func readVersion(ctx *armadacontext.ArmadaContext, db Querier) (int, error) {
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

func setVersion(ctx *armadacontext.ArmadaContext, db Querier, version int) error {
	_, err := db.Exec(ctx, `SELECT setval('database_version', $1)`, version)
	return err
}

func ReadMigrations(fsys fs.FS, basePath string) ([]Migration, error) {
	files, err := fs.ReadDir(fsys, basePath)
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	var migrations []Migration
	for _, f := range files {

		if f.IsDir() {
			continue
		}

		bytes, err := fs.ReadFile(fsys, path.Join(basePath, f.Name()))
		if err != nil {
			return nil, err
		}

		id, err := strconv.Atoi(strings.Split(f.Name(), "_")[0])
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, Migration{
			id:   id,
			name: f.Name(),
			sql:  string(bytes),
		})
	}
	return migrations, nil
}

// TODO: remove this when we've migrated over to iofs
func ReadMigrationsFromStatik(namespace string) ([]Migration, error) {
	vfs, err := stakikfs.NewWithNamespace(namespace)
	if err != nil {
		return nil, err
	}

	dir, err := vfs.Open("/")
	if err != nil {
		return nil, err
	}

	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	var migrations []Migration
	for _, f := range files {
		file, err := vfs.Open("/" + f.Name())
		if err != nil {
			return nil, err
		}
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(file)
		if err != nil {
			return nil, err
		}
		id, err := strconv.Atoi(strings.Split(f.Name(), "_")[0])
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, Migration{
			id:   id,
			name: f.Name(),
			sql:  buf.String(),
		})
	}
	return migrations, nil
}

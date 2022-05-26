package database

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgtype/pgxtype"

	"github.com/rakyll/statik/fs"
	log "github.com/sirupsen/logrus"
)

type migration struct {
	id   int
	name string
	sql  string
}

func UpdateDatabase(ctx context.Context, db pgxtype.Querier, migrations []migration) error {
	log.Info("Updating postgres...")
	version, err := readVersion(ctx, db)
	if err != nil {
		return err
	}
	log.Infof("Current version %v", version)

	for _, m := range migrations {
		if m.id > version {
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

func readVersion(ctx context.Context, db pgxtype.Querier) (int, error) {
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

func setVersion(ctx context.Context, db pgxtype.Querier, version int) error {
	_, err := db.Exec(ctx, `SELECT setval('database_version', $1)`, version)
	return err
}

func GetMigrations(namespace string) ([]migration, error) {
	vfs, err := fs.NewWithNamespace(namespace)
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

	migrations := []migration{}
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
		migrations = append(migrations, migration{
			id:   id,
			name: f.Name(),
			sql:  buf.String(),
		})
	}
	return migrations, nil
}

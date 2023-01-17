package schema

import (
	"bytes"
	"context"
	"database/sql"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rakyll/statik/fs"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/lookout/repository/schema/statik"
)

type migration struct {
	id   int
	name string
	sql  string
}

func UpdateDatabase(db *sql.DB) (err error) {
	log.Info("Updating database...")

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			if commitErr := tx.Commit(); commitErr != nil {
				err = commitErr
			}
		} else {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = errors.WithMessage(err, rollbackErr.Error())
			}
		}
	}()

	err = obtainLock(tx)
	if err != nil {
		return err
	}

	version, err := readVersion(tx)
	if err != nil {
		return err
	}

	log.Infof("Current version %v", version)
	migrations, err := getMigrations()
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if m.id > version {
			_, err := tx.Exec(m.sql)
			if err != nil {
				return err
			}

			version = m.id
			err = setVersion(tx, version)
			if err != nil {
				return err
			}
		}
	}
	log.Info("Database updated.")
	return nil
}

func readVersion(tx *sql.Tx) (int, error) {
	_, err := tx.Exec(
		`CREATE SEQUENCE IF NOT EXISTS database_version START WITH 0 MINVALUE 0;`)
	if err != nil {
		return 0, err
	}

	result, err := tx.Query(
		`SELECT last_value FROM database_version`)
	if err != nil {
		return 0, err
	}

	var version int
	result.Next()
	err = result.Scan(&version)
	result.Close()

	return version, err
}

func obtainLock(tx *sql.Tx) error {
	_, err := tx.Exec(
		`CREATE TABLE IF NOT EXISTS migration_lock (a int);`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`LOCK TABLE migration_lock IN ACCESS EXCLUSIVE MODE;`)
	if err != nil {
		return err
	}
	return nil
}

func setVersion(tx *sql.Tx, version int) error {
	_, err := tx.Exec(`SELECT setval('database_version', $1)`, version)
	return err
}

func getMigrations() ([]migration, error) {
	vfs, err := fs.NewWithNamespace(statik.LookoutSql)
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

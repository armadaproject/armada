package pgkeyvalue

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/logging"
)

type KeyValue struct {
	Key      string    `db:"key"`
	Value    []byte    `db:"value"`
	Inserted time.Time `db:"inserted"`
}

// PGKeyValueStore is a time-limited key-value store backed by postgres with a local LRU cache.
// The store is write-only, i.e., writing to an existing key will return an error (of type *armadaerrors.ErrAlreadyExists).
// Keys can only be deleted by running the cleanup function.
// Deleting keys does not cause caches to update, i.e., nodes may have an inconsistent view if keys are deleted.
type PGKeyValueStore struct {
	// Postgres connection.
	db *pgxpool.Pool
	// Name of the postgres table used for storage.
	tableName string
	// Used to set inserted time
	clock clock.Clock
}

func New(ctx *armadacontext.ArmadaContext, db *pgxpool.Pool, tableName string) (*PGKeyValueStore, error) {
	if db == nil {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "db",
			Value:   db,
			Message: "db must be non-nil",
		})
	}
	if tableName == "" {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "TableName",
			Value:   tableName,
			Message: "TableName must be non-empty",
		})
	}
	err := createTableIfNotExists(ctx, db, tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PGKeyValueStore{
		db:        db,
		tableName: tableName,
		clock:     clock.RealClock{},
	}, nil
}

func (c *PGKeyValueStore) Load(ctx *armadacontext.ArmadaContext, keys []string) (map[string][]byte, error) {
	rows, err := c.db.Query(ctx, fmt.Sprintf("SELECT KEY, VALUE FROM %s WHERE KEY = any($1)", c.tableName), keys)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	kv := make(map[string][]byte, len(keys))
	for rows.Next() {
		key := ""
		var value []byte = nil
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		kv[key] = value
	}
	return kv, nil
}

func (c *PGKeyValueStore) Store(ctx *armadacontext.ArmadaContext, kvs map[string][]byte) error {
	data := make([]KeyValue, 0, len(kvs))
	for k, v := range kvs {
		data = append(data, KeyValue{
			Key:      k,
			Value:    v,
			Inserted: c.clock.Now(),
		})
	}
	return database.UpsertWithTransaction(ctx, c.db, c.tableName, data)
}

func createTableIfNotExists(ctx *armadacontext.ArmadaContext, db *pgxpool.Pool, tableName string) error {
	_, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		    key TEXT PRIMARY KEY,
		    value BYTEA,
		    inserted TIMESTAMP not null
	);`, tableName))
	return err
}

// Cleanup removes all key-value pairs older than lifespan.
func (c *PGKeyValueStore) cleanup(ctx *armadacontext.ArmadaContext, lifespan time.Duration) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE (inserted <= $1);", c.tableName)
	_, err := c.db.Exec(ctx, sql, c.clock.Now().Add(-lifespan))
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// PeriodicCleanup starts a goroutine that automatically runs the cleanup job
// every interval until the provided context is cancelled.
func (c *PGKeyValueStore) PeriodicCleanup(ctx *armadacontext.ArmadaContext, interval time.Duration, lifespan time.Duration) error {
	log := logrus.StandardLogger().WithField("service", "PGKeyValueStoreCleanup")
	log.Info("service started")
	ticker := c.clock.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil
		case <-ticker.C():
			start := time.Now()
			err := c.cleanup(ctx, lifespan)
			if err != nil {
				logging.WithStacktrace(log, err).WithField("delay", time.Since(start)).Warn("cleanup failed")
			} else {
				log.WithField("delay", c.clock.Since(start)).Info("cleanup succeeded")
			}
		}
	}
}

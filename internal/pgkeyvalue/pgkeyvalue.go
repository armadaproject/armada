package pgkeyvalue

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/logging"
)

// PGKeyValueStore is a time-limited key-value store backed by postgres with a local LRU cache.
// The store is write-only, i.e., writing to an existing key will return an error (of type *armadaerrors.ErrAlreadyExists).
// Keys can only be deleted by running the cleanup function.
// Deleting keys does not cause caches to update, i.e., nodes may have an inconsistent view if keys are deleted.
type PGKeyValueStore struct {
	// For performance, keys are cached locally.
	cache *simplelru.LRU
	// Postgres connection.
	db *pgxpool.Pool
	// Name of the postgres table used for storage.
	tableName string
}

func New(db *pgxpool.Pool, cacheSize int, tableName string) (*PGKeyValueStore, error) {
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
	cache, err := simplelru.NewLRU(cacheSize, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PGKeyValueStore{
		cache:     cache,
		db:        db,
		tableName: tableName,
	}, nil
}

// Add adds a key-value pair. Returns true if successful and false if the key already exists.
// The posgres table backing the key-value storage is created automatically if it doesn't already exist.
func (c *PGKeyValueStore) Add(ctx context.Context, key string, value []byte) (bool, error) {
	ok, err := c.add(ctx, key, value)

	// If the table doesn't exist, create it and try again.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedTable { // Relation doesn't exist; create it.
		c.createTable(ctx)
		ok, err = c.add(ctx, key, value)
	}

	return ok, err
}

// AddKey is equivalent to Add(ctx, key, nil).
func (c *PGKeyValueStore) AddKey(ctx context.Context, key string) (bool, error) {
	return c.Add(ctx, key, nil)
}

func (c *PGKeyValueStore) createTable(ctx context.Context) error {
	var pgErr *pgconn.PgError
	_, err := c.db.Exec(ctx, fmt.Sprintf("create table %s (key text primary key, value bytea, inserted timestamp not null);", c.tableName))
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.DuplicateTable { // Someone else just created it, which is fine.
		return nil
	}
	return err
}

func (c *PGKeyValueStore) add(ctx context.Context, key string, value []byte) (bool, error) {

	// Overwriting isn't allowed.
	if _, ok := c.cache.Get(key); ok {
		return false, nil
	}

	// Otherwise, get and set the key in a transaction.
	var exists *bool
	err := c.db.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {

		// Check if the key already exists in postgres.
		sql := fmt.Sprintf("select exists(select 1 from %s where key=$1) AS \"exists\"", c.tableName)
		err := tx.QueryRow(ctx, sql, key).Scan(&exists)
		if err != nil {
			return err
		}

		// Only write the key-value pair if it doesn't already exist (overwriting not allowed).
		if !*exists {
			sql = fmt.Sprintf("insert into %s (key, value, inserted) values ($1, $2, now());", c.tableName)
			_, err := tx.Exec(ctx, sql, key, value)
			if err != nil {
				return err
			}
		}

		return nil
	})

	// We need to return on error (in particular tx rollback)
	// to avoid writing to the cache after failing to write to postgres.
	if err != nil {
		return false, errors.WithStack(err)
	}

	// Only add to cache if we also wrote to postgres.
	if *exists {
		return false, nil
	} else {
		c.cache.Add(key, value)
	}

	return true, nil
}

// Get returns the value associated with the provided key,
// or &armadaerrors.ErrNotFound if the key can't be found.
func (c *PGKeyValueStore) Get(ctx context.Context, key string) ([]byte, error) {

	// First check the local cache.
	if value, ok := c.cache.Get(key); ok {
		return value.([]byte), nil
	}

	// Otherwise, check postgres.
	var exists bool
	var value *[]byte
	sql := fmt.Sprintf("select value from %s where key=$1", c.tableName)
	err := c.db.QueryRow(ctx, sql, key).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		exists = false
	} else if err != nil {
		return nil, errors.WithStack(err)
	} else {
		exists = true
	}

	if !exists {
		return nil, errors.WithStack(&armadaerrors.ErrNotFound{
			Type:  "Postgres key-value pair",
			Value: key,
		})
	}

	return *value, nil
}

// Cleanup removes all key-value pairs older than lifespan.
func (c *PGKeyValueStore) Cleanup(ctx context.Context, lifespan time.Duration) error {
	sql := fmt.Sprintf("delete from %s where (inserted <= (now() - $1::interval));", c.tableName)
	_, err := c.db.Exec(ctx, sql, lifespan)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// PeriodicCleanup starts a goroutine that automatically runs the cleanup job
// every interval until the provided context is cancelled.
func (c *PGKeyValueStore) PeriodicCleanup(ctx context.Context, interval time.Duration, lifespan time.Duration) {
	log := logrus.StandardLogger().WithField("service", "PGKeyValueStoreCleanup")
	log.Info("service started")
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				start := time.Now()
				err := c.Cleanup(ctx, lifespan)
				if err != nil {
					logging.WithStacktrace(log, err).WithField("delay", time.Since(start)).Warn("cleanup failed")
				} else {
					log.WithField("delay", time.Since(start)).Info("cleanup succeeded")
				}
			}
		}
	}()
}

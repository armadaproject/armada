package pgkeyvalue

import (
	"context"
	"fmt"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/logging"
)

type KeyValue struct {
	Key   string
	Value []byte
}

// PGKeyValueStore is a time-limited key-value store backed by postgres with a local LRU cache.
// The store is write-only, i.e., writing to an existing key will return an error (of type *armadaerrors.ErrAlreadyExists).
// Keys can only be deleted by running the cleanup function.
// Deleting keys does not cause caches to update, i.e., nodes may have an inconsistent view if keys are deleted.
type PGKeyValueStore struct {
	// For performance, keys are cached locally.
	cache *lru.Cache
	// Postgres connection.
	db *pgxpool.Pool
	// Name of the postgres table used for storage.
	tableName string
	Logger    *logrus.Logger
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
	cache, err := lru.New(cacheSize)
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
// The postgres table backing the key-value storage is created automatically if it doesn't already exist.
func (c *PGKeyValueStore) Add(ctx context.Context, key string, value []byte) (bool, error) {
	ok, err := c.add(ctx, key, value)

	// If the table doesn't exist, create it and try again.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedTable { // Relation doesn't exist; create it.
		if err := c.createTable(ctx); err != nil {
			return false, errors.WithStack(err)
		}
		ok, err = c.add(ctx, key, value)
	}

	return ok, err
}

// LoadOrStoreBatch returns the existing values for the supplied keys if present.  Otherwise, it stores and returns
// the supplied value.
// The postgres table backing the key-value storage is created automatically if it doesn't already exist.
func (c *PGKeyValueStore) LoadOrStoreBatch(ctx context.Context, batch []*KeyValue) (map[string][]byte, error) {
	ret, err := c.addBatch(ctx, batch)

	// If the table doesn't exist, create it and try again.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedTable { // Relation doesn't exist; create it.
		if err := c.createTable(ctx); err != nil {
			return nil, errors.WithStack(err)
		}
		ret, err = c.addBatch(ctx, batch)
	}
	return ret, err
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

func (c *PGKeyValueStore) addBatch(ctx context.Context, batch []*KeyValue) (map[string][]byte, error) {
	addedByKey := map[string][]byte{}
	keysToAdd := map[string][]byte{}

	// first check the cache to see if we have added anything
	for _, kv := range batch {
		if val, ok := c.cache.Get(kv.Key); ok {
			addedByKey[kv.Key] = val.([]byte)
		} else {
			keysToAdd[kv.Key] = kv.Value
		}
	}

	if len(addedByKey) == len(batch) {
		return addedByKey, nil
	}

	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]interface{}, 0, len(batch)*3)
	i := 0
	now := time.Now().UTC()
	for k, v := range keysToAdd {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		valueArgs = append(valueArgs, k)
		valueArgs = append(valueArgs, v)
		valueArgs = append(valueArgs, now)
		i++
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %s (key, value, inserted) VALUES %s ON CONFLICT (key) DO UPDATE SET key=EXCLUDED.key  RETURNING key, value",
		c.tableName,
		strings.Join(valueStrings, ","))
	rows, err := c.db.Query(ctx, stmt, valueArgs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for rows.Next() {
		key := ""
		var value []byte = nil
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		c.cache.Add(key, value)
		addedByKey[key] = value
	}

	return addedByKey, nil
}

func (c *PGKeyValueStore) add(ctx context.Context, key string, value []byte) (bool, error) {
	// Overwriting isn't allowed.
	if _, ok := c.cache.Get(key); ok {
		return false, nil
	}

	// Otherwise, get and set the key in a transaction.
	var exists *bool
	tx, err := c.db.BeginTx(ctx, pgx.TxOptions{})
	// We need to return on error (in particular tx rollback)
	// to avoid writing to the cache after failing to write to postgres.
	if err != nil {
		return false, errors.WithStack(err)
	}
	// Check if the key already exists in postgres.
	sql := fmt.Sprintf("select exists(select 1 from %s where key=$1) AS \"exists\"", c.tableName)
	if err := tx.QueryRow(ctx, sql, key).Scan(&exists); err != nil {
		return false, errors.WithStack(err)
	}

	// Only write the key-value pair if it doesn't already exist (overwriting not allowed).
	if !*exists {
		sql = fmt.Sprintf("insert into %s (key, value, inserted) values ($1, $2, now());", c.tableName)
		_, err := tx.Exec(ctx, sql, key, value)
		if err != nil {
			return false, errors.WithStack(err)
		}
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
func (c *PGKeyValueStore) PeriodicCleanup(ctx context.Context, interval time.Duration, lifespan time.Duration) error {
	var log *logrus.Entry
	if c.Logger == nil {
		log = logrus.StandardLogger().WithField("service", "PGKeyValueStoreCleanup")
	} else {
		log = c.Logger.WithField("service", "PGKeyValueStoreCleanup")
	}

	log.Info("service started")
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil
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
}

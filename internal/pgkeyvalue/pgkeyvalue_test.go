package pgkeyvalue

import (
	"context"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/lookout/testutil"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	testutil.WithDatabasePgx(t, func(db *pgxpool.Pool) {
		cache, err := New(db, "cachetable")
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Adding a key for the first time should insert into both the local cache and postgres.
		err = cache.Add(context.Background(), "foo", []byte{0, 1, 2})
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// The second time we add the key, we should get an error.
		var targetErr *armadaerrors.ErrAlreadyExists
		err = cache.Add(context.Background(), "foo", []byte{0, 1, 2})
		assert.ErrorAs(t, err, &targetErr)

		// Adding another key should succeed.
		err = cache.Add(context.Background(), "bar", []byte{0, 1, 2})
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Clear the local cache to verify that it queries postgres.
		cache.cache.Purge()
		err = cache.Add(context.Background(), "foo", []byte{0, 1, 2})
		assert.ErrorAs(t, err, &targetErr)
	})
}

func TestAddGet(t *testing.T) {
	testutil.WithDatabasePgx(t, func(db *pgxpool.Pool) {
		cache, err := New(db, "cachetable")
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Adding a key for the first time should insert into both the local cache and postgres.
		expected := []byte{0, 1, 2}
		err = cache.Add(context.Background(), "foo", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Get should return the same value
		actual, err := cache.Get(context.Background(), "foo")
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.Equal(t, expected, actual)

		// Getting another value should return *armadaerrors.ErrNotFound
		var targetErr *armadaerrors.ErrNotFound
		actual, err = cache.Get(context.Background(), "bar")
		assert.ErrorAs(t, err, &targetErr)

		// Purging the cache should still return the same value for foo
		cache.cache.Purge()
		actual, err = cache.Get(context.Background(), "foo")
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.Equal(t, expected, actual)
	})
}

func TestCleanup(t *testing.T) {
	testutil.WithDatabasePgx(t, func(db *pgxpool.Pool) {
		cache, err := New(db, "cachetable")
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Adding a key for the first time should insert into both the local cache and postgres,
		// and return false (since the key didn't already exist).
		expected := []byte{0, 1, 2}
		err = cache.Add(context.Background(), "foo", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Run the cleanup.
		err = cache.Cleanup(context.Background(), 0*time.Second)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Purge the cache to ensure the next get will query postgres.
		cache.cache.Purge()

		// The key should've been cleaned up and get should return an error.
		var targetErr *armadaerrors.ErrNotFound
		_, err = cache.Get(context.Background(), "foo")
		assert.ErrorAs(t, err, &targetErr)

		// Add another key
		err = cache.Add(context.Background(), "bar", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// The cleanup shouldn't delete this key
		err = cache.Cleanup(context.Background(), time.Hour)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		cache.cache.Purge()
		_, err = cache.Get(context.Background(), "bar")
		assert.NoError(t, err)
	})
}

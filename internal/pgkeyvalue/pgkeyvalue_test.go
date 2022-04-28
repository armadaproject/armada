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
		store, err := New(db, "cachetable")
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Adding a key for the first time should insert into both the local cache and postgres.
		ok, err := store.Add(context.Background(), "foo", []byte{0, 1, 2})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		// The second time we add the key, we should get an error.
		ok, err = store.Add(context.Background(), "foo", []byte{0, 1, 2})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.False(t, ok)

		// Adding another key should succeed.
		ok, err = store.Add(context.Background(), "bar", []byte{0, 1, 2})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		// Clear the local cache to verify that it queries postgres.
		store.cache.Purge()
		ok, err = store.Add(context.Background(), "foo", []byte{0, 1, 2})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.False(t, ok)

		// Test AddKey
		ok, err = store.AddKey(context.Background(), "baz")
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		ok, err = store.AddKey(context.Background(), "baz")
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.False(t, ok)
	})
}

func TestAddGet(t *testing.T) {
	testutil.WithDatabasePgx(t, func(db *pgxpool.Pool) {
		store, err := New(db, "cachetable")
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Adding a key for the first time should insert into both the local cache and postgres.
		expected := []byte{0, 1, 2}
		ok, err := store.Add(context.Background(), "foo", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		// Get should return the same value
		actual, err := store.Get(context.Background(), "foo")
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.Equal(t, expected, actual)

		// Getting another value should return *armadaerrors.ErrNotFound
		var targetErr *armadaerrors.ErrNotFound
		actual, err = store.Get(context.Background(), "bar")
		assert.ErrorAs(t, err, &targetErr)

		// Purging the cache should still return the same value for foo
		store.cache.Purge()
		actual, err = store.Get(context.Background(), "foo")
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.Equal(t, expected, actual)
	})
}

func TestCleanup(t *testing.T) {
	testutil.WithDatabasePgx(t, func(db *pgxpool.Pool) {
		store, err := New(db, "cachetable")
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Adding a key for the first time should insert into both the local cache and postgres,
		// and return false (since the key didn't already exist).
		expected := []byte{0, 1, 2}
		ok, err := store.Add(context.Background(), "foo", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		// Run the cleanup.
		err = store.Cleanup(context.Background(), 0*time.Second)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Purge the cache to ensure the next get will query postgres.
		store.cache.Purge()

		// The key should've been cleaned up and get should return an error.
		var targetErr *armadaerrors.ErrNotFound
		_, err = store.Get(context.Background(), "foo")
		assert.ErrorAs(t, err, &targetErr)

		// Add another key
		ok, err = store.Add(context.Background(), "bar", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		// The cleanup shouldn't delete this key
		err = store.Cleanup(context.Background(), time.Hour)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		store.cache.Purge()
		_, err = store.Get(context.Background(), "bar")
		assert.NoError(t, err)

		// Test the automatic cleanup
		ok, err = store.Add(context.Background(), "baz", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		// Start the cleanup job to run a quick interval.
		// Then try adding baz twice more to make sure it gets cleaned up both times.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.PeriodicCleanup(ctx, time.Microsecond, time.Microsecond)

		time.Sleep(100 * time.Millisecond)
		store.cache.Purge()

		ok, err = store.Add(context.Background(), "baz", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)

		time.Sleep(100 * time.Millisecond)
		store.cache.Purge()

		ok, err = store.Add(context.Background(), "baz", expected)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.True(t, ok)
	})
}

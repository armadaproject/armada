package pgkeyvalue

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/lookout/testutil"
)

func TestAdd(t *testing.T) {
	cacheSize := 100
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		store, err := New(db, cacheSize, "cachetable")
		require.NoError(t, err)

		// Adding a key for the first time should insert into both the local cache and postgres.
		ok, err := store.Add(context.Background(), "foo", []byte{0, 1, 2})
		require.NoError(t, err)
		assert.True(t, ok)

		// The second time we add the key, we should get an error.
		ok, err = store.Add(context.Background(), "foo", []byte{0, 1, 2})
		require.NoError(t, err)
		assert.False(t, ok)

		// Adding another key should succeed.
		ok, err = store.Add(context.Background(), "bar", []byte{0, 1, 2})
		require.NoError(t, err)
		assert.True(t, ok)

		// Clear the local cache to verify that it queries postgres.
		store.cache.Purge()
		ok, err = store.Add(context.Background(), "foo", []byte{0, 1, 2})
		require.NoError(t, err)
		assert.False(t, ok)

		// Test AddKey
		ok, err = store.AddKey(context.Background(), "baz")
		require.NoError(t, err)
		assert.True(t, ok)

		ok, err = store.AddKey(context.Background(), "baz")
		require.NoError(t, err)
		assert.False(t, ok)
		return nil
	})
	require.NoError(t, err)
}

func TestLoadOrStoreBatch(t *testing.T) {
	cacheSize := 100
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		store, err := New(db, cacheSize, "cachetable")
		require.NoError(t, err)

		// Add two items
		kv1 := []*KeyValue{
			{"foo", []byte{0x1}},
			{"bar", []byte{0x2}},
		}
		expected1 := map[string][]byte{"foo": {0x1}, "bar": {0x2}}
		added, err := store.LoadOrStoreBatch(context.Background(), kv1)
		require.NoError(t, err)
		assert.Equal(t, expected1, added)

		// Add items again
		added, err = store.LoadOrStoreBatch(context.Background(), kv1)
		require.NoError(t, err)
		assert.Equal(t, expected1, added)

		// Add three items
		kv2 := []*KeyValue{
			{"foo", []byte{0x1}},
			{"bar", []byte{0x2}},
			{"baz", []byte{0x3}},
		}
		expected2 := map[string][]byte{"foo": {0x1}, "bar": {0x2}, "baz": {0x3}}

		// Asset that only one is added
		added, err = store.LoadOrStoreBatch(context.Background(), kv2)
		require.NoError(t, err)
		assert.Equal(t, added, expected2)

		// Add a duplicate
		kv3 := []*KeyValue{
			{"foo", []byte{0x4}},
			{"bar", []byte{0x5}},
		}
		expected3 := map[string][]byte{"foo": {0x1}, "bar": {0x2}}
		added, err = store.LoadOrStoreBatch(context.Background(), kv3)
		require.NoError(t, err)
		assert.Equal(t, added, expected3)

		return nil
	})
	assert.NoError(t, err)
}

func TestAddGet(t *testing.T) {
	cacheSize := 100
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		store, err := New(db, cacheSize, "cachetable")
		require.NoError(t, err)
		// Adding a key for the first time should insert into both the local cache and postgres.
		expected := []byte{0, 1, 2}
		ok, err := store.Add(context.Background(), "foo", expected)
		require.NoError(t, err)
		assert.True(t, ok)

		// Get should return the same value
		actual, err := store.Get(context.Background(), "foo")
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		// Getting another value should return *armadaerrors.ErrNotFound
		var targetErr *armadaerrors.ErrNotFound
		_, err = store.Get(context.Background(), "bar")
		assert.ErrorAs(t, err, &targetErr)

		// Purging the cache should still return the same value for foo
		store.cache.Purge()
		actual, err = store.Get(context.Background(), "foo")
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
		return nil
	})
	require.NoError(t, err)
}

func TestCleanup(t *testing.T) {
	cacheSize := 100
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		store, err := New(db, cacheSize, "cachetable")
		require.NoError(t, err)

		// Set an empty logger to avoid annoying "cleanup succeeded" messages
		store.Logger = &logrus.Logger{}

		// Adding a key for the first time should insert into both the local cache and postgres,
		// and return false (since the key didn't already exist).
		expected := []byte{0, 1, 2}
		ok, err := store.Add(context.Background(), "foo", expected)
		require.NoError(t, err)
		assert.True(t, ok)

		// Run the cleanup.
		err = store.Cleanup(context.Background(), 0*time.Second)
		require.NoError(t, err)

		// Purge the cache to ensure the next get will query postgres.
		store.cache.Purge()

		// The key should've been cleaned up and get should return an error.
		var targetErr *armadaerrors.ErrNotFound
		_, err = store.Get(context.Background(), "foo")
		assert.ErrorAs(t, err, &targetErr)

		// Add another key
		ok, err = store.Add(context.Background(), "bar", expected)
		require.NoError(t, err)
		assert.True(t, ok)

		// The cleanup shouldn't delete this key
		err = store.Cleanup(context.Background(), time.Hour)
		require.NoError(t, err)
		store.cache.Purge()
		_, err = store.Get(context.Background(), "bar")
		require.NoError(t, err)

		// Test the automatic cleanup
		ok, err = store.Add(context.Background(), "baz", expected)
		require.NoError(t, err)
		assert.True(t, ok)

		// Start the cleanup job to run a quick interval.
		// Then try adding baz twice more to make sure it gets cleaned up both times.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			err := store.PeriodicCleanup(ctx, time.Microsecond, time.Microsecond)
			assert.NoError(t, err)
		}()

		time.Sleep(100 * time.Millisecond)
		store.cache.Purge()

		ok, err = store.Add(context.Background(), "baz", expected)
		require.NoError(t, err)
		assert.True(t, ok)

		time.Sleep(100 * time.Millisecond)
		store.cache.Purge()

		ok, err = store.Add(context.Background(), "baz", expected)
		require.NoError(t, err)
		assert.True(t, ok)
		return nil
	})
	require.NoError(t, err)
}

func BenchmarkStore(b *testing.B) {
	cacheSize := 100
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		store, err := New(db, cacheSize, "cachetable")
		require.NoError(b, err)
		for i := 0; i < b.N; i++ {
			_, err := store.AddKey(context.Background(), "foo")
			require.NoError(b, err)
		}
		return nil
	})
	require.NoError(b, err)
}

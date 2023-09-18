package pgkeyvalue

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/lookout/testutil"
)

func TestLoadStore(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		kvStore, err := New(ctx, db, "cachetable")
		require.NoError(t, err)

		data1 := map[string][]byte{
			"a": {0x1}, "b": {0x2}, "c": {0x3},
		}
		err = kvStore.Store(ctx, data1)
		require.NoError(t, err)

		loaded, err := kvStore.Load(ctx, maps.Keys(data1))
		require.NoError(t, err)
		assert.Equal(t, data1, loaded)

		data2 := map[string][]byte{"c": {0x4}, "d": {0x5}}
		err = kvStore.Store(ctx, data2)
		require.NoError(t, err)

		loaded, err = kvStore.Load(ctx, []string{"a", "b", "c", "d"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]byte{
			"a": {0x1}, "b": {0x2}, "c": {0x4}, "d": {0x5},
		}, loaded)

		return nil
	})
	require.NoError(t, err)
}

func TestCleanup(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		baseTime := time.Now()
		testClock := clock.NewFakeClock(baseTime)
		kvStore, err := New(ctx, db, "cachetable")
		kvStore.clock = testClock
		require.NoError(t, err)

		// Data that will be cleaned up
		data1 := map[string][]byte{"a": {0x1}, "b": {0x2}}
		err = kvStore.Store(ctx, data1)
		require.NoError(t, err)

		// advance the clock
		testClock.SetTime(testClock.Now().Add(5 * time.Second))

		// Data that won't be cleaned up
		data2 := map[string][]byte{"c": {0x3}}
		err = kvStore.Store(ctx, data2)
		require.NoError(t, err)

		loaded, err := kvStore.Load(ctx, []string{"a", "b", "c"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]byte{
			"a": {0x1}, "b": {0x2}, "c": {0x3},
		}, loaded)

		// Run the cleanup.
		err = kvStore.cleanup(ctx, 5*time.Second)
		require.NoError(t, err)

		loaded, err = kvStore.Load(ctx, []string{"a", "b", "c"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]byte{"c": {0x3}}, loaded)
		return nil
	})
	require.NoError(t, err)
}

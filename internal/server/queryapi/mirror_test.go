package queryapi

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	dbcommon "github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
	lookouthcschema "github.com/armadaproject/armada/internal/lookouthc/schema"
	"github.com/armadaproject/armada/internal/server/queryapi/database"
	"github.com/armadaproject/armada/pkg/api"
)

// withPrimaryAndMirrorDbs runs action with two independent test databases: a
// primary carrying the standard lookout schema and a mirror carrying the
// experimental hot-cold partitioned schema.
func withPrimaryAndMirrorDbs(t *testing.T, action func(primary, mirror *pgxpool.Pool)) {
	t.Helper()
	migrations, err := lookoutschema.LookoutMigrations()
	require.NoError(t, err)

	err = dbcommon.WithTestDb(migrations, func(primary *pgxpool.Pool) error {
		return dbcommon.WithTestDb(migrations, func(mirror *pgxpool.Pool) error {
			require.NoError(t, lookouthcschema.ApplyPartitioner(armadacontext.Background(), mirror))
			action(primary, mirror)
			return nil
		})
	})
	require.NoError(t, err)
}

// waitForMirrorQuery blocks until the mirror pool's acquire count has advanced
// past baseline (i.e. a mirrored query has run) or the deadline elapses.
func waitForMirrorQuery(t *testing.T, mirror *pgxpool.Pool, baseline int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if mirror.Stat().AcquireCount() > baseline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("mirror pool was not queried within the deadline")
}

func TestMirroringDB_ReplaysQueryAndPreservesPrimaryResult(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testJobs := []database.Job{
		newJob("job1", lookout.JobRunningOrdinal, ""),
		newJob("job2", lookout.JobSucceededOrdinal, ""),
	}

	withPrimaryAndMirrorDbs(t, func(primary, mirror *pgxpool.Pool) {
		require.NoError(t, dbcommon.UpsertPartitionedWithTransaction(ctx, primary, "job", []string{"job_id"}, testJobs))
		require.NoError(t, dbcommon.UpsertPartitionedWithTransaction(ctx, mirror, "job", []string{"job_id"}, testJobs))

		request := &api.JobStatusRequest{JobIds: []string{"job1", "job2"}}

		baseline := New(primary, defaultMaxQueryItems, testDecompressor)
		baselineResp, err := baseline.GetJobStatus(ctx, request)
		require.NoError(t, err)

		mirrorAcquiresBefore := mirror.Stat().AcquireCount()
		mirrored := New(NewMirroringDB(primary, mirror, 0), defaultMaxQueryItems, testDecompressor)
		mirroredResp, err := mirrored.GetJobStatus(ctx, request)
		require.NoError(t, err)

		assert.Equal(t, baselineResp, mirroredResp)
		waitForMirrorQuery(t, mirror, mirrorAcquiresBefore)
	})
}

func TestMirroringDB_ReplaysTransactionalQuery(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testJobs := []database.Job{
		newJob("job1", lookout.JobRunningOrdinal, "run1"),
	}
	testJobRuns := []database.JobRun{
		newJobRun("job1", "run1", lookout.JobRunRunningOrdinal, baseTime, "", testIngressAddresses),
	}

	withPrimaryAndMirrorDbs(t, func(primary, mirror *pgxpool.Pool) {
		for _, db := range []*pgxpool.Pool{primary, mirror} {
			require.NoError(t, dbcommon.UpsertPartitionedWithTransaction(ctx, db, "job", []string{"job_id"}, testJobs))
			require.NoError(t, dbcommon.UpsertPartitionedWithTransaction(ctx, db, "job_run", []string{"run_id"}, testJobRuns))
		}

		request := &api.JobDetailsRequest{JobIds: []string{"job1"}, ExpandJobRun: true}

		baseline := New(primary, defaultMaxQueryItems, testDecompressor)
		baselineResp, err := baseline.GetJobDetails(ctx, request)
		require.NoError(t, err)

		mirrorAcquiresBefore := mirror.Stat().AcquireCount()
		mirrored := New(NewMirroringDB(primary, mirror, 0), defaultMaxQueryItems, testDecompressor)
		mirroredResp, err := mirrored.GetJobDetails(ctx, request)
		require.NoError(t, err)

		assert.Equal(t, baselineResp, mirroredResp)
		// GetJobDetails runs inside a read-only transaction, so this exercises
		// the mirroringTx replay path.
		waitForMirrorQuery(t, mirror, mirrorAcquiresBefore)
	})
}

func TestMirroringDB_BrokenMirrorDoesNotAffectPrimary(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testJobs := []database.Job{
		newJob("job1", lookout.JobRunningOrdinal, ""),
	}

	withPrimaryAndMirrorDbs(t, func(primary, mirror *pgxpool.Pool) {
		require.NoError(t, dbcommon.UpsertPartitionedWithTransaction(ctx, primary, "job", []string{"job_id"}, testJobs))
		// Close the mirror pool so every replayed query fails.
		mirror.Close()

		request := &api.JobStatusRequest{JobIds: []string{"job1"}}

		baseline := New(primary, defaultMaxQueryItems, testDecompressor)
		baselineResp, err := baseline.GetJobStatus(ctx, request)
		require.NoError(t, err)

		mirrored := New(NewMirroringDB(primary, mirror, 0), defaultMaxQueryItems, testDecompressor)
		mirroredResp, err := mirrored.GetJobStatus(ctx, request)
		require.NoError(t, err)

		assert.Equal(t, baselineResp, mirroredResp)
	})
}

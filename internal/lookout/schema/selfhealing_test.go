package schema_test

import (
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	lookoutmodel "github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/internal/lookout/pruner"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookout/schema"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
	queryapidb "github.com/armadaproject/armada/internal/server/queryapi/database"
)

var (
	jobTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Every error is fatal, so a statement that errors instead of waiting for
	// the conversion fails the test rather than being retried.
	everyErrorIsFatal = []*regexp.Regexp{regexp.MustCompile(".")}
)

func seedJobs(t *testing.T, db *pgxpool.Pool, n int) {
	t.Helper()
	_, err := db.Exec(armadacontext.Background(), `
		INSERT INTO job (job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu, priority, submitted,
		                 state, last_transition_time, last_transition_time_seconds, annotations)
		SELECT format('seed-%s', i), 'queue', 'owner', 'jobset', 1, 1, 1, 0, 0, $2, 1 + i % 9, $2, 0, '{}'
		FROM generate_series(1, $1) AS i
	`, n, jobTime)
	require.NoError(t, err)
}

func createJob(jobId string) *model.CreateJobInstruction {
	return &model.CreateJobInstruction{
		JobId:                     jobId,
		Queue:                     "queue",
		Owner:                     "owner",
		JobSet:                    "jobset",
		Submitted:                 jobTime,
		State:                     lookout.JobQueuedOrdinal,
		LastTransitionTime:        jobTime,
		LastTransitionTimeSeconds: jobTime.Unix(),
		Annotations:               map[string]string{},
	}
}

func updateJobState(jobId string, state int32) *model.UpdateJobInstruction {
	return &model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     ptr.To(state),
		LastTransitionTime:        &jobTime,
		LastTransitionTimeSeconds: ptr.To(jobTime.Unix()),
	}
}

// TestExecutePartitionJobTable_SelfHealsWhileIngestingAndQuerying converts
// the job table while the ingester writes to it and the Lookout API and
// Query API read it, using long-lived pools whose prepared statements were
// cached against the unpartitioned table. Statements blocked by the
// conversion must wait, then succeed against the partitioned table, and no
// writes may be lost.
func TestExecutePartitionJobTable_SelfHealsWhileIngestingAndQuerying(t *testing.T) {
	ctx := armadacontext.Background()
	migrations, err := schema.LookoutMigrations()
	require.NoError(t, err)

	err = database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		// Enough rows that the conversion holds its lock for a while.
		const seeded = 200_000
		seedJobs(t, db, seeded)

		store := lookoutdb.NewLookoutDb(db, everyErrorIsFatal, metrics.Get(), 10, 10)
		getJobsRepo := repository.NewSqlGetJobsRepository(db)
		queryApi := queryapidb.New(db)

		var written, read atomic.Int64
		stop := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 1; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				instructions := &model.InstructionSet{
					JobsToCreate: []*model.CreateJobInstruction{createJob(fmt.Sprintf("live-%d", i))},
					MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(i)},
				}
				if i > 1 {
					// Moves the previous job from job_active to job_terminated once partitioned.
					instructions.JobsToUpdate = []*model.UpdateJobInstruction{updateJobState(fmt.Sprintf("live-%d", i-1), lookout.JobSucceededOrdinal)}
				}
				if err := store.Store(ctx, instructions); err != nil {
					errs <- fmt.Errorf("ingester: %w", err)
					return
				}
				written.Store(int64(i))
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := getJobsRepo.GetJobs(ctx, []*lookoutmodel.Filter{}, false, &lookoutmodel.Order{Field: "jobId", Direction: "DESC"}, 0, 10); err != nil {
					errs <- fmt.Errorf("lookout API: %w", err)
					return
				}
				if _, err := queryApi.GetJobStates(ctx, []string{"seed-1", "live-1"}); err != nil {
					errs <- fmt.Errorf("query API: %w", err)
					return
				}
				read.Add(1)
			}
		}()

		waitFor := func(counter *atomic.Int64, atLeast int64) {
			require.Eventually(t, func() bool { return counter.Load() >= atLeast }, 30*time.Second, 10*time.Millisecond)
		}
		waitFor(&written, 20)
		waitFor(&read, 20)

		var sawBlocked atomic.Bool
		conversionDone := make(chan struct{})
		go func() {
			for {
				select {
				case <-conversionDone:
					return
				case <-time.After(10 * time.Millisecond):
				}
				var blocked bool
				err := db.QueryRow(ctx, `
					SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND locktype = 'relation'
					AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))
				`).Scan(&blocked)
				if err == nil && blocked {
					sawBlocked.Store(true)
				}
			}
		}()

		conn, err := db.Acquire(ctx)
		require.NoError(t, err)
		converted, err := schema.ExecutePartitionJobTable(ctx, conn.Conn(), schema.ExecuteOptions{
			LockTimeout: 10 * time.Second, LockAttempts: 3, ProgressInterval: time.Minute,
		})
		conn.Release()
		close(conversionDone)
		require.NoError(t, err)
		require.True(t, converted)

		writtenAtSwap, readAtSwap := written.Load(), read.Load()
		waitFor(&written, writtenAtSwap+20)
		waitFor(&read, readAtSwap+20)
		close(stop)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assert.True(t, sawBlocked.Load(), "the ingester or queries should have waited for the conversion")

		last := written.Load()
		var total, liveActive, liveTerminated int64
		require.NoError(t, db.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM job),
			       (SELECT count(*) FROM job_active WHERE job_id LIKE 'live-%'),
			       (SELECT count(*) FROM job_terminated WHERE job_id LIKE 'live-%' AND state = $1)
		`, lookout.JobSucceededOrdinal).Scan(&total, &liveActive, &liveTerminated))
		assert.Equal(t, seeded+last, total, "no job may be lost or duplicated")
		assert.Equal(t, int64(1), liveActive, "only the last job is still queued")
		assert.Equal(t, last-1, liveTerminated)
		return nil
	})
	require.NoError(t, err)
}

// TestExecutePartitionJobTable_MigratedRowsCanBeUpdatedAndPruned verifies that
// rows that existed before the conversion behave like any other afterwards.
func TestExecutePartitionJobTable_MigratedRowsCanBeUpdatedAndPruned(t *testing.T) {
	ctx := armadacontext.Background()
	migrations, err := schema.LookoutMigrations()
	require.NoError(t, err)

	err = database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		seedJobs(t, db, 90)
		conn, err := db.Acquire(ctx)
		require.NoError(t, err)
		defer conn.Release()
		converted, err := schema.ExecutePartitionJobTable(ctx, conn.Conn(), schema.ExecuteOptions{LockTimeout: 10 * time.Second, LockAttempts: 1, ProgressInterval: time.Minute})
		require.NoError(t, err)
		require.True(t, converted)

		// seed-9 is queued (state 1 + 9 % 9); succeeding it moves it into job_terminated.
		store := lookoutdb.NewLookoutDb(db, everyErrorIsFatal, metrics.Get(), 10, 10)
		require.NoError(t, store.Store(ctx, &model.InstructionSet{
			JobsToUpdate: []*model.UpdateJobInstruction{updateJobState("seed-9", lookout.JobSucceededOrdinal)},
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}))
		var partition string
		require.NoError(t, db.QueryRow(ctx, `SELECT tableoid::regclass::text FROM job WHERE job_id = 'seed-9'`).Scan(&partition))
		assert.Equal(t, "job_terminated", partition)

		require.NoError(t, pruner.PruneDb(ctx, conn.Conn(), time.Hour, time.Hour, 0, 10, clock.RealClock{}))
		var active, terminated int
		require.NoError(t, db.QueryRow(ctx, `SELECT (SELECT count(*) FROM job_active), (SELECT count(*) FROM job_terminated)`).Scan(&active, &terminated))
		assert.Equal(t, 39, active, "the pruner must not delete active jobs")
		assert.Equal(t, 0, terminated, "the pruner should delete expired terminated jobs, including migrated ones")
		return nil
	})
	require.NoError(t, err)
}

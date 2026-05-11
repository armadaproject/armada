package lookoutdb

import (
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
	lookouthcschema "github.com/armadaproject/armada/internal/lookouthc/schema"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
)

func withLookoutHCDb(action func(db *pgxpool.Pool) error) error {
	migrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		if err := lookouthcschema.ApplyPartitioner(armadacontext.Background(), db); err != nil {
			return err
		}
		return action(db)
	})
}

func countInPartition(t *testing.T, db *pgxpool.Pool, partition, jobId string) int {
	t.Helper()
	var count int
	err := db.QueryRow(
		armadacontext.Background(),
		"SELECT count(*) FROM "+partition+" WHERE job_id = $1",
		jobId,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

func TestHotCold_StoreRoutesTerminalJobToTerminatedPartition(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		createInstructions := &model.InstructionSet{
			JobsToCreate: []*model.CreateJobInstruction{makeCreateJobInstruction(JobId)},
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), createInstructions))
		assert.Equal(t, 1, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 0, countInPartition(t, db, "job_terminated", JobId))

		runningUpdate := &model.InstructionSet{
			JobsToUpdate: []*model.UpdateJobInstruction{{
				JobId:                     JobId,
				State:                     ptr.To[int32](lookout.JobRunningOrdinal),
				LastTransitionTime:        &updateTime,
				LastTransitionTimeSeconds: ptr.To(updateTime.Unix()),
			}},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(2)},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), runningUpdate))
		assert.Equal(t, 1, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 0, countInPartition(t, db, "job_terminated", JobId))

		succeededUpdate := &model.InstructionSet{
			JobsToUpdate: []*model.UpdateJobInstruction{{
				JobId:                     JobId,
				State:                     ptr.To[int32](lookout.JobSucceededOrdinal),
				LastTransitionTime:        &finishedTime,
				LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
			}},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(3)},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), succeededUpdate))
		assert.Equal(t, 0, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 1, countInPartition(t, db, "job_terminated", JobId))

		job := getJob(t, db, JobId)
		assert.Equal(t, lookout.JobSucceededOrdinal, int(job.State))

		// job_spec is an unpartitioned table; its rows are unaffected by
		// state transitions that route the job between partitions.
		jobSpec := getJobSpec(t, db, JobId)
		assert.Equal(t, JobId, jobSpec.JobId)
		assert.Equal(t, []byte(jobProto), jobSpec.JobProto)

		var jobSpecRelkind byte
		require.NoError(t, db.QueryRow(armadacontext.Background(),
			"SELECT relkind FROM pg_class WHERE relname = 'job_spec'").Scan(&jobSpecRelkind))
		assert.Equal(t, byte('r'), jobSpecRelkind, "job_spec must be an ordinary (non-partitioned) table")
		return nil
	})
	require.NoError(t, err)
}

func TestHotCold_StoreKeepsRunningJobInActivePartition(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		instructions := &model.InstructionSet{
			JobsToCreate: []*model.CreateJobInstruction{makeCreateJobInstruction(JobId)},
			JobsToUpdate: []*model.UpdateJobInstruction{{
				JobId:                     JobId,
				State:                     ptr.To[int32](lookout.JobRunningOrdinal),
				LastTransitionTime:        &updateTime,
				LastTransitionTimeSeconds: ptr.To(updateTime.Unix()),
			}},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), instructions))

		assert.Equal(t, 1, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 0, countInPartition(t, db, "job_terminated", JobId))

		job := getJob(t, db, JobId)
		assert.Equal(t, int32(lookout.JobRunningOrdinal), job.State)
		return nil
	})
	require.NoError(t, err)
}

func TestHotCold_MultipleJobsDistributedAcrossPartitions(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		activeIds := []string{"job-active-1", "job-active-2"}
		terminalTargets := map[string]int32{
			"job-succeeded": lookout.JobSucceededOrdinal,
			"job-failed":    lookout.JobFailedOrdinal,
			"job-cancelled": lookout.JobCancelledOrdinal,
			"job-preempted": lookout.JobPreemptedOrdinal,
			"job-rejected":  lookout.JobRejectedOrdinal,
		}

		var createInstructions []*model.CreateJobInstruction
		for _, id := range activeIds {
			createInstructions = append(createInstructions, makeCreateJobInstruction(id))
		}
		for id := range terminalTargets {
			createInstructions = append(createInstructions, makeCreateJobInstruction(id))
		}

		require.NoError(t, ldb.Store(armadacontext.Background(), &model.InstructionSet{
			JobsToCreate: createInstructions,
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}))

		var updates []*model.UpdateJobInstruction
		for _, id := range activeIds {
			updates = append(updates, &model.UpdateJobInstruction{
				JobId:                     id,
				State:                     ptr.To[int32](lookout.JobRunningOrdinal),
				LastTransitionTime:        &updateTime,
				LastTransitionTimeSeconds: ptr.To(updateTime.Unix()),
			})
		}
		for id, state := range terminalTargets {
			updates = append(updates, &model.UpdateJobInstruction{
				JobId:                     id,
				State:                     ptr.To(state),
				LastTransitionTime:        &finishedTime,
				LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
			})
		}

		require.NoError(t, ldb.Store(armadacontext.Background(), &model.InstructionSet{
			JobsToUpdate: updates,
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(2)},
		}))

		for _, id := range activeIds {
			assert.Equal(t, 1, countInPartition(t, db, "job_active", id), "expected %s in job_active", id)
			assert.Equal(t, 0, countInPartition(t, db, "job_terminated", id), "expected %s not in job_terminated", id)
		}
		for id := range terminalTargets {
			assert.Equal(t, 0, countInPartition(t, db, "job_active", id), "expected %s not in job_active", id)
			assert.Equal(t, 1, countInPartition(t, db, "job_terminated", id), "expected %s in job_terminated", id)
		}

		var activeCount, terminatedCount int
		require.NoError(t, db.QueryRow(armadacontext.Background(),
			"SELECT count(*) FROM job_active").Scan(&activeCount))
		require.NoError(t, db.QueryRow(armadacontext.Background(),
			"SELECT count(*) FROM job_terminated").Scan(&terminatedCount))
		assert.Equal(t, len(activeIds), activeCount)
		assert.Equal(t, len(terminalTargets), terminatedCount)
		return nil
	})
	require.NoError(t, err)
}

func TestHotCold_FailedJobStoresErrorAndRoutesToTerminatedPartition(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		instructions := &model.InstructionSet{
			JobsToCreate: []*model.CreateJobInstruction{makeCreateJobInstruction(JobId)},
			JobsToUpdate: []*model.UpdateJobInstruction{{
				JobId:                     JobId,
				State:                     ptr.To[int32](lookout.JobFailedOrdinal),
				LastTransitionTime:        &finishedTime,
				LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
			}},
			JobErrorsToCreate: []*model.CreateJobErrorInstruction{{
				JobId: JobId,
				Error: []byte(testfixtures.ErrMsg),
			}},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), instructions))

		assert.Equal(t, 0, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 1, countInPartition(t, db, "job_terminated", JobId))

		jobError := getJobError(t, db, JobId)
		assert.Equal(t, expectedJobError, jobError)
		return nil
	})
	require.NoError(t, err)
}

func TestHotCold_ParentJobTableReturnsAllJobs(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		createInstructions := []*model.CreateJobInstruction{
			makeCreateJobInstruction("job-a"),
			makeCreateJobInstruction("job-b"),
			makeCreateJobInstruction("job-c"),
			makeCreateJobInstruction("job-d"),
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), &model.InstructionSet{
			JobsToCreate: createInstructions,
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}))

		updates := []*model.UpdateJobInstruction{
			{
				JobId:                     "job-b",
				State:                     ptr.To[int32](lookout.JobRunningOrdinal),
				LastTransitionTime:        &updateTime,
				LastTransitionTimeSeconds: ptr.To(updateTime.Unix()),
			},
			{
				JobId:                     "job-c",
				State:                     ptr.To[int32](lookout.JobSucceededOrdinal),
				LastTransitionTime:        &finishedTime,
				LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
			},
			{
				JobId:                     "job-d",
				State:                     ptr.To[int32](lookout.JobFailedOrdinal),
				LastTransitionTime:        &finishedTime,
				LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
			},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), &model.InstructionSet{
			JobsToUpdate: updates,
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(2)},
		}))

		rows, err := db.Query(armadacontext.Background(),
			"SELECT job_id FROM job ORDER BY job_id")
		require.NoError(t, err)
		defer rows.Close()

		var ids []string
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"job-a", "job-b", "job-c", "job-d"}, ids)
		return nil
	})
	require.NoError(t, err)
}

func TestHotCold_TerminalStateQueryPrunesActivePartition(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		require.NoError(t, ldb.Store(armadacontext.Background(), &model.InstructionSet{
			JobsToCreate: []*model.CreateJobInstruction{
				makeCreateJobInstruction("job-active"),
				makeCreateJobInstruction("job-terminal"),
			},
			JobsToUpdate: []*model.UpdateJobInstruction{{
				JobId:                     "job-terminal",
				State:                     ptr.To[int32](lookout.JobSucceededOrdinal),
				LastTransitionTime:        &finishedTime,
				LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
			}},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}))

		rows, err := db.Query(armadacontext.Background(),
			// terminal: Succeeded=4, Failed=5, Cancelled=6, Preempted=7, Rejected=9
			"EXPLAIN (ANALYZE false) SELECT * FROM job WHERE state IN (4,5,6,7,9)")
		require.NoError(t, err)
		defer rows.Close()

		var plan strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			plan.WriteString(line)
			plan.WriteString("\n")
		}
		require.NoError(t, rows.Err())

		planText := plan.String()
		assert.Contains(t, planText, "job_terminated")
		assert.NotContains(t, planText, "job_active")
		return nil
	})
	require.NoError(t, err)
}

func TestHotCold_ConflatedTerminalUpdatesProduceSingleTerminatedRow(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10, 10)

		require.NoError(t, ldb.Store(armadacontext.Background(), &model.InstructionSet{
			JobsToCreate: []*model.CreateJobInstruction{makeCreateJobInstruction(JobId)},
			MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		}))

		assert.Equal(t, 1, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 0, countInPartition(t, db, "job_terminated", JobId))

		batched := &model.InstructionSet{
			JobsToUpdate: []*model.UpdateJobInstruction{
				{
					JobId:                     JobId,
					State:                     ptr.To[int32](lookout.JobRunningOrdinal),
					LastTransitionTime:        &updateTime,
					LastTransitionTimeSeconds: ptr.To(updateTime.Unix()),
				},
				{
					JobId:                     JobId,
					State:                     ptr.To[int32](lookout.JobFailedOrdinal),
					LastTransitionTime:        &finishedTime,
					LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
				},
				{
					JobId:                     JobId,
					State:                     ptr.To[int32](lookout.JobSucceededOrdinal),
					LastTransitionTime:        &finishedTime,
					LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
				},
				{
					JobId:                     JobId,
					State:                     ptr.To[int32](lookout.JobCancelledOrdinal),
					LastTransitionTime:        &finishedTime,
					LastTransitionTimeSeconds: ptr.To(finishedTime.Unix()),
				},
			},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(2)},
		}
		require.NoError(t, ldb.Store(armadacontext.Background(), batched))

		assert.Equal(t, 0, countInPartition(t, db, "job_active", JobId))
		assert.Equal(t, 1, countInPartition(t, db, "job_terminated", JobId))

		var totalInParent int
		require.NoError(t, db.QueryRow(armadacontext.Background(),
			"SELECT count(*) FROM job WHERE job_id = $1", JobId).Scan(&totalInParent))
		assert.Equal(t, 1, totalInParent)
		return nil
	})
	require.NoError(t, err)
}

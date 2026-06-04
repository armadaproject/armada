package scheduleringester

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

// TestInsertJobs_MigrationPhases verifies that an InsertJobs op routes
// submit_message and groups to the correct table(s) based on the configured
// migration phase of the SchedulerDb.
func TestInsertJobs_MigrationPhases(t *testing.T) {
	cases := []struct {
		name                 string
		phase                schedulerdb.JobMetadataMigrationPhase
		wantJobsHasBlobs     bool
		wantMetadataRowWrote bool
	}{
		{"legacy", schedulerdb.JobMetadataMigrationPhaseLegacy, true, false},
		{"dualWrite", schedulerdb.JobMetadataMigrationPhaseDualWrite, true, true},
		{"cutover", schedulerdb.JobMetadataMigrationPhaseCutover, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, schedulerdb.WithTestDb(func(_ *schedulerdb.Queries, db *pgxpool.Pool) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
				defer cancel()

				sdb := &SchedulerDb{db: db, migrationPhase: tc.phase}

				jobID := "job-" + string(tc.phase)
				submitMessage := []byte("submit-" + string(tc.phase))
				groups := []byte("groups-" + string(tc.phase))

				op := InsertJobs{jobID: &schedulerdb.Job{
					JobID:          jobID,
					JobSet:         "set",
					Queue:          "queue",
					SchedulingInfo: []byte{},
					SubmitMessage:  submitMessage,
					Groups:         groups,
				}}

				require.NoError(t, pgx.BeginTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
					return sdb.WriteDbOp(ctx, tx, op)
				}))

				var jobSubmitMessage, jobGroups []byte
				require.NoError(t, db.QueryRow(ctx,
					"SELECT submit_message, groups FROM jobs WHERE job_id = $1", jobID,
				).Scan(&jobSubmitMessage, &jobGroups))

				if tc.wantJobsHasBlobs {
					assert.Equal(t, submitMessage, jobSubmitMessage)
					assert.Equal(t, groups, jobGroups)
				} else {
					assert.Nil(t, jobSubmitMessage)
					assert.Nil(t, jobGroups)
				}

				var metadataCount int
				require.NoError(t, db.QueryRow(ctx,
					"SELECT COUNT(*) FROM job_metadata WHERE job_id = $1", jobID,
				).Scan(&metadataCount))

				if tc.wantMetadataRowWrote {
					require.Equal(t, 1, metadataCount)
					var metadataSubmitMessage, metadataGroups []byte
					require.NoError(t, db.QueryRow(ctx,
						"SELECT submit_message, groups FROM job_metadata WHERE job_id = $1", jobID,
					).Scan(&metadataSubmitMessage, &metadataGroups))
					assert.Equal(t, submitMessage, metadataSubmitMessage)
					assert.Equal(t, groups, metadataGroups)
				} else {
					assert.Equal(t, 0, metadataCount)
				}

				return nil
			}))
		})
	}
}

// TestInsertJobs_CutoverPreservesLegacyColumns verifies that upserting an
// existing jobs row in cutover phase does not overwrite its legacy
// submit_message/groups columns (they must stay readable for any not-yet-
// backfilled rows).
func TestInsertJobs_CutoverPreservesLegacyColumns(t *testing.T) {
	require.NoError(t, schedulerdb.WithTestDb(func(_ *schedulerdb.Queries, db *pgxpool.Pool) error {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
		defer cancel()

		legacySdb := &SchedulerDb{db: db, migrationPhase: schedulerdb.JobMetadataMigrationPhaseLegacy}
		cutoverSdb := &SchedulerDb{db: db, migrationPhase: schedulerdb.JobMetadataMigrationPhaseCutover}

		jobID := "job-preserve"
		legacySubmitMessage := []byte("legacy-sm")
		legacyGroups := []byte("legacy-g")

		legacyOp := InsertJobs{jobID: &schedulerdb.Job{
			JobID:          jobID,
			JobSet:         "set",
			Queue:          "queue",
			SchedulingInfo: []byte{},
			SubmitMessage:  legacySubmitMessage,
			Groups:         legacyGroups,
		}}
		require.NoError(t, pgx.BeginTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return legacySdb.WriteDbOp(ctx, tx, legacyOp)
		}))

		// Re-upsert the same job in cutover phase. Legacy columns must be untouched.
		cutoverOp := InsertJobs{jobID: &schedulerdb.Job{
			JobID:          jobID,
			JobSet:         "set",
			Queue:          "queue",
			SchedulingInfo: []byte{},
			SubmitMessage:  []byte("should-not-be-written"),
			Groups:         []byte("should-not-be-written"),
		}}
		require.NoError(t, pgx.BeginTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return cutoverSdb.WriteDbOp(ctx, tx, cutoverOp)
		}))

		var jobSubmitMessage, jobGroups []byte
		require.NoError(t, db.QueryRow(ctx,
			"SELECT submit_message, groups FROM jobs WHERE job_id = $1", jobID,
		).Scan(&jobSubmitMessage, &jobGroups))
		assert.Equal(t, legacySubmitMessage, jobSubmitMessage)
		assert.Equal(t, legacyGroups, jobGroups)

		return nil
	}))
}

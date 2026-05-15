package database

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
)

func TestFetchJobRunLeases_MigrationPhases(t *testing.T) {
	const executorName = "test-executor"

	legacyJobID := util.NewULID()
	migratedJobID := util.NewULID()
	bothJobID := util.NewULID()

	legacyGroups := []byte("legacy-groups")
	legacySubmit := []byte("legacy-submit")
	migratedGroups := []byte("migrated-groups")
	migratedSubmit := []byte("migrated-submit")
	bothGroupsOnJobs := []byte("both-jobs-groups")
	bothSubmitOnJobs := []byte("both-jobs-submit")
	bothGroupsOnSpec := []byte("both-spec-groups")
	bothSubmitOnSpec := []byte("both-spec-submit")

	seed := func(ctx *armadacontext.Context, db *pgxpool.Pool) ([]Run, error) {
		legacyJob := Job{JobID: legacyJobID, JobSet: "js", Queue: "q", Groups: legacyGroups, SubmitMessage: legacySubmit, SchedulingInfo: []byte{}}
		migratedJob := Job{JobID: migratedJobID, JobSet: "js", Queue: "q", SchedulingInfo: []byte{}}
		bothJob := Job{JobID: bothJobID, JobSet: "js", Queue: "q", Groups: bothGroupsOnJobs, SubmitMessage: bothSubmitOnJobs, SchedulingInfo: []byte{}}
		if err := upsertJobs(ctx, db, []Job{legacyJob, migratedJob, bothJob}); err != nil {
			return nil, err
		}
		// Seed job_specs for migrated + both
		for _, r := range []struct {
			jobID, groups, submit []byte
		}{
			{[]byte(migratedJobID), migratedGroups, migratedSubmit},
			{[]byte(bothJobID), bothGroupsOnSpec, bothSubmitOnSpec},
		} {
			if _, err := db.Exec(ctx,
				"INSERT INTO job_specs (job_id, groups, submit_message) VALUES ($1, $2, $3)",
				string(r.jobID), r.groups, r.submit,
			); err != nil {
				return nil, err
			}
		}
		// Clear jobs.submit_message for migrated row
		if _, err := db.Exec(ctx,
			"UPDATE jobs SET submit_message = NULL, groups = NULL WHERE job_id = $1",
			migratedJobID,
		); err != nil {
			return nil, err
		}
		runs := []Run{
			{RunID: uuid.NewString(), JobID: legacyJobID, JobSet: "js", Executor: executorName, Pool: "p"},
			{RunID: uuid.NewString(), JobID: migratedJobID, JobSet: "js", Executor: executorName, Pool: "p"},
			{RunID: uuid.NewString(), JobID: bothJobID, JobSet: "js", Executor: executorName, Pool: "p"},
		}
		return runs, upsertRuns(ctx, db, runs)
	}

	type expected struct {
		groups []byte
		submit []byte
	}

	cases := []struct {
		name  string
		phase JobSpecMigrationPhase
		want  map[string]expected
	}{
		{
			name:  "dualWrite prefers job_specs when present",
			phase: JobSpecMigrationPhaseDualWrite,
			want: map[string]expected{
				legacyJobID:   {legacyGroups, legacySubmit},         // falls back to jobs
				migratedJobID: {migratedGroups, migratedSubmit},     // from job_specs
				bothJobID:     {bothGroupsOnSpec, bothSubmitOnSpec}, // job_specs wins
			},
		},
		{
			name:  "legacy phase reads only the jobs columns",
			phase: JobSpecMigrationPhaseLegacy,
			want: map[string]expected{
				legacyJobID:   {legacyGroups, legacySubmit},
				migratedJobID: {nil, nil},
				bothJobID:     {bothGroupsOnJobs, bothSubmitOnJobs},
			},
		},
		{
			name:  "cutover phase reads only the job_specs columns",
			phase: JobSpecMigrationPhaseCutover,
			want: map[string]expected{
				legacyJobID:   {nil, nil},
				migratedJobID: {migratedGroups, migratedSubmit},
				bothJobID:     {bothGroupsOnSpec, bothSubmitOnSpec},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
				defer cancel()

				runs, err := seed(ctx, db)
				require.NoError(t, err)

				repo := NewPostgresJobRepository(db, defaultBatchSize, tc.phase)
				leases, err := repo.FetchJobRunLeases(ctx, executorName, 100, nil)
				require.NoError(t, err)

				runByJobID := map[string]string{}
				for _, r := range runs {
					runByJobID[r.JobID] = r.RunID
				}
				got := map[string]expected{}
				for _, lease := range leases {
					// Invert run-id -> job-id
					for jobID, runID := range runByJobID {
						if runID == lease.RunID {
							got[jobID] = expected{lease.Groups, lease.SubmitMessage}
						}
					}
				}
				for jobID, want := range tc.want {
					assert.Equal(t, want, got[jobID], "job %s", jobID)
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestPruner_DeletesJobSpecsRows(t *testing.T) {
	err := WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		jobID := util.NewULID()
		terminatedJob := Job{
			JobID: jobID, JobSet: "js", Queue: "q", SchedulingInfo: []byte{},
			Succeeded: true,
		}
		require.NoError(t, upsertJobs(ctx, db, []Job{terminatedJob}))
		_, err := db.Exec(ctx,
			"INSERT INTO job_specs (job_id, submit_message, groups) VALUES ($1, $2, $3)",
			jobID, []byte("submit"), []byte("groups"),
		)
		require.NoError(t, err)

		// Backdate last_modified so the pruner considers this row.
		_, err = db.Exec(ctx,
			"UPDATE jobs SET last_modified = now() - interval '1 day' WHERE job_id = $1", jobID)
		require.NoError(t, err)

		conn, err := db.Acquire(ctx)
		require.NoError(t, err)
		defer conn.Release()
		require.NoError(t, PruneDb(ctx, conn.Conn(), 100, time.Minute, clocktesting.NewFakeClock(time.Now().Add(time.Hour))))

		var specCount int
		require.NoError(t, db.QueryRow(ctx, "SELECT count(*) FROM job_specs WHERE job_id = $1", jobID).Scan(&specCount))
		assert.Equal(t, 0, specCount, "pruner should delete the job_specs row")

		var jobCount int
		require.NoError(t, db.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE job_id = $1", jobID).Scan(&jobCount))
		assert.Equal(t, 0, jobCount, "pruner should delete the jobs row")
		return nil
	})
	require.NoError(t, err)
}

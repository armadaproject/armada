package database

import (
	"context"
	"fmt"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/database"
)

func TestPruneDb(t *testing.T) {
	baseTime := time.Now().UTC()
	defaultKeepAfterDuration := 1 * time.Hour
	expiredJobTime := baseTime.Add(-defaultKeepAfterDuration)
	tests := map[string]struct {
		jobs                     []Job
		runs                     []Run
		expectedJobsPostPrune    []string
		expectedJobRunsPostPrune []uuid.UUID
	}{
		"remove succeeded job": {
			jobs: []Job{{
				JobID:        "test-job",
				Succeeded:    true,
				LastModified: expiredJobTime,
			}},
			expectedJobsPostPrune: []string{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				testClock := clock.NewFakeClock(baseTime)

				// Set up db
				jobsToInsert := slices.Map(tc.jobs, populateRequiredJobFields)
				err := removeTriggers(ctx, db)
				require.NoError(t, err)
				err = database.Upsert(ctx, db, "jobs", jobsToInsert)
				require.NoError(t, err)
				err = database.Upsert(ctx, db, "runs", tc.runs)
				require.NoError(t, err)
				queries := New(db)

				err = PruneDb(ctx, db, defaultKeepAfterDuration, testClock)
				require.NoError(t, err)

				remainingJobs, err := queries.SelectAllJobIds(ctx)
				require.NoError(t, err)

				remainingJobRuns, err := queries.SelectAllRunIds(ctx)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedJobsPostPrune, remainingJobs)
				assert.Equal(t, tc.expectedJobRunsPostPrune, remainingJobRuns)

				return nil
			})
		})
	}
}

// Removes the triggers that auto-set serial and last_update_time as
// we need to manipulate these as part of the test
func removeTriggers(ctx context.Context, db *pgxpool.Pool) error {
	triggers := map[string]string{
		"jobs":           "next_serial_on_insert_jobs",
		"runs":           "next_serial_on_insert_runs",
		"job_run_errors": "next_serial_on_insert_job_run_errors",
	}

	for table, trigger := range triggers {
		_, err := db.Exec(ctx, fmt.Sprintf("DROP TRIGGER %s on %s;", trigger, table))
		if err != nil {
			return err
		}
	}
	return nil
}

func populateRequiredJobFields(job Job) Job {
	job.JobSet = "test-jobset"
	job.Queue = "test-queue"
	job.UserID = ""
	job.Submitted = 0
	job.SubmitMessage = []byte{}
	job.SchedulingInfo = []byte{}
	return job
}

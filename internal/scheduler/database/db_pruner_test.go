package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/slices"
	commonutil "github.com/armadaproject/armada/internal/common/util"
)

func TestPruneDb_RemoveJobs(t *testing.T) {
	baseTime := time.Now().UTC()
	defaultKeepAfterDuration := 1 * time.Hour
	expiredJobTime := baseTime.Add(-defaultKeepAfterDuration).Add(-time.Second)
	defaultBatchSize := 2
	tests := map[string]struct {
		jobs                     []Job
		runs                     []Run
		errors                   []JobRunError
		expectedJobsPostPrune    []string
		expectedJobRunsPostPrune []uuid.UUID
		expectedErrorsPostPrune  []JobRunError
	}{
		"remove succeeded job": {
			jobs: []Job{{
				JobID:        "test-job",
				Succeeded:    true,
				LastModified: expiredJobTime,
			}},
			expectedJobsPostPrune: nil,
		},
		"remove failed job": {
			jobs: []Job{{
				JobID:        "test-job",
				Failed:       true,
				LastModified: expiredJobTime,
			}},
			expectedJobsPostPrune: nil,
		},
		"remove cancelled job": {
			jobs: []Job{{
				JobID:        "test-job",
				Cancelled:    true,
				LastModified: expiredJobTime,
			}},
			expectedJobsPostPrune: nil,
		},
		"don't remove non-terminal job": {
			jobs: []Job{{
				JobID:        "test-job",
				LastModified: expiredJobTime,
			}},
			expectedJobsPostPrune: []string{"test-job"},
		},
		"don't remove recently finished job": {
			jobs: []Job{{
				JobID:        "test-job",
				Cancelled:    true,
				LastModified: baseTime.Add(-defaultKeepAfterDuration).Add(1 * time.Second),
			}},
			expectedJobsPostPrune: []string{"test-job"},
		},
		"remove job runs": {
			jobs: []Job{{
				JobID:        "test-job",
				Cancelled:    true,
				LastModified: expiredJobTime,
			}},
			runs: []Run{{
				RunID:        uuid.New(),
				JobID:        "test-job",
				JobSet:       "test-jobset",
				Running:      true,
				Serial:       0,
				LastModified: baseTime,
			}},
			expectedJobsPostPrune:    nil,
			expectedJobRunsPostPrune: nil,
		},
		"remove job errors": {
			jobs: []Job{{
				JobID:        "test-job",
				Cancelled:    true,
				LastModified: expiredJobTime,
			}},
			errors: []JobRunError{{
				RunID: uuid.New(),
				JobID: "test-job",
				Error: []byte{0x1},
			}},
			expectedJobsPostPrune:   nil,
			expectedErrorsPostPrune: nil,
		},
		"remove lots of jobs in batches": {
			jobs:                     make100CompletedJobs(expiredJobTime),
			expectedJobsPostPrune:    nil,
			expectedJobRunsPostPrune: nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
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
				err = database.Upsert(ctx, db, "job_run_errors", tc.errors)
				require.NoError(t, err)
				queries := New(db)
				dbConn, err := db.Acquire(ctx)
				require.NoError(t, err)
				err = PruneDb(ctx, dbConn.Conn(), defaultBatchSize, defaultKeepAfterDuration, testClock)
				require.NoError(t, err)

				remainingJobs, err := queries.SelectAllJobIds(ctx)
				require.NoError(t, err)

				remainingJobRuns, err := queries.SelectAllRunIds(ctx)
				require.NoError(t, err)

				remainingJobErrors, err := queries.SelectAllRunErrors(ctx)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedJobsPostPrune, remainingJobs)
				assert.Equal(t, tc.expectedJobRunsPostPrune, remainingJobRuns)
				assert.Equal(t, tc.expectedErrorsPostPrune, remainingJobErrors)

				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func TestPruneDb_RemoveMarkers(t *testing.T) {
	baseTime := time.Now().UTC()
	defaultKeepAfterDuration := 1 * time.Hour
	expiredMarkerTime := baseTime.Add(-defaultKeepAfterDuration).Add(-time.Second).UTC()
	defaultGroupId := uuid.New()
	tests := map[string]struct {
		markers                  []Marker
		expectedMarkersPostPrune []Marker
	}{
		"remove old marker": {
			markers: []Marker{
				{
					GroupID:     defaultGroupId,
					PartitionID: 1,
					Created:     expiredMarkerTime,
				},
			},
		},
		"don't remove new marker": {
			markers: []Marker{
				{
					GroupID:     defaultGroupId,
					PartitionID: 1,
					Created:     baseTime,
				},
			},
			expectedMarkersPostPrune: []Marker{
				{
					GroupID:     defaultGroupId,
					PartitionID: 1,
					Created:     baseTime,
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				testClock := clock.NewFakeClock(baseTime)

				// Set up db
				for _, m := range tc.markers {
					_, err := db.Exec(ctx,
						"INSERT INTO markers VALUES ($1, $2, $3)", m.GroupID, m.PartitionID, m.Created)
					require.NoError(t, err)
				}

				queries := New(db)
				dbConn, err := db.Acquire(ctx)
				require.NoError(t, err)
				err = PruneDb(ctx, dbConn.Conn(), defaultBatchSize, defaultKeepAfterDuration, testClock)
				require.NoError(t, err)

				remainingMarkers, err := queries.SelectAllMarkers(ctx)
				require.NoError(t, err)

				// database creates times as local
				for i, marker := range remainingMarkers {
					remainingMarkers[i].Created = marker.Created.UTC()
				}
				assert.Equal(t, tc.expectedMarkersPostPrune, remainingMarkers)
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

// Removes the triggers that auto-set serial and last_update_time as
// we need to manipulate these as part of the test
func removeTriggers(ctx context.Context, db *pgxpool.Pool) error {
	triggers := map[string]string{
		"jobs": "next_serial_on_insert_jobs",
		"runs": "next_serial_on_insert_runs",
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

func make100CompletedJobs(lastModified time.Time) []Job {
	jobs := make([]Job, 100)
	for i := 0; i < len(jobs); i++ {
		jobs[i] = Job{
			JobID:        commonutil.NewULID(),
			Cancelled:    true,
			LastModified: lastModified,
			Serial:       int64(i),
		}
	}
	return jobs
}

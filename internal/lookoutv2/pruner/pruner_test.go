package pruner

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutv2/repository"
)

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

type testJob struct {
	jobId string
	ts    time.Time
	state lookout.JobState
}

func TestPruneDb(t *testing.T) {
	type testCase struct {
		testName    string
		expireAfter time.Duration
		jobs        []testJob
		jobIdsLeft  []string
	}

	nIds := 100
	sampleJobIds := make([]string, nIds)
	for i := 0; i < nIds; i++ {
		sampleJobIds[i] = util.NewULID()
	}

	manyJobs := func(startIdx, endIdx int, state lookout.JobState, ts time.Time) []testJob {
		var testJobs []testJob
		for i := startIdx; i < endIdx; i++ {
			testJobs = append(testJobs, testJob{
				jobId: sampleJobIds[i],
				ts:    ts,
				state: state,
			})
		}
		return testJobs
	}

	testCases := []testCase{
		{
			testName:    "no jobs",
			expireAfter: 10 * time.Hour,
			jobs:        []testJob{},
			jobIdsLeft:  []string{},
		},
		{
			testName:    "no expired jobs",
			expireAfter: 10 * time.Hour,
			jobs: []testJob{
				// Terminated jobs within the expiry
				{
					jobId: sampleJobIds[0],
					ts:    baseTime,
					state: lookout.JobSucceeded,
				},
				{
					jobId: sampleJobIds[1],
					ts:    baseTime.Add(-9 * time.Hour),
					state: lookout.JobSucceeded,
				},
				// Non-terminated job older than the expiry
				{
					jobId: sampleJobIds[2],
					ts:    baseTime.Add(-11 * time.Hour),
					state: lookout.JobRunning,
				},
			},
			jobIdsLeft: []string{sampleJobIds[0], sampleJobIds[1], sampleJobIds[2]},
		},
		{
			testName:    "expire a job",
			expireAfter: 10 * time.Hour,
			jobs: []testJob{
				{
					jobId: sampleJobIds[0],
					ts:    baseTime.Add(-(10*time.Hour + 1*time.Minute)),
					state: lookout.JobSucceeded,
				},
				{
					jobId: sampleJobIds[1],
					ts:    baseTime.Add(-(10*time.Hour + 1*time.Minute)),
					state: lookout.JobFailed,
				},
				{
					jobId: sampleJobIds[2],
					ts:    baseTime.Add(-(10*time.Hour + 1*time.Minute)),
					state: lookout.JobCancelled,
				},
				{
					jobId: sampleJobIds[3],
					ts:    baseTime.Add(-(10*time.Hour + 1*time.Minute)),
					state: lookout.JobPreempted,
				},
			},
			jobIdsLeft: []string{},
		},
		{
			testName:    "expire many jobs",
			expireAfter: 100 * time.Hour,
			jobs: slices.Concatenate(
				manyJobs(0, 10, lookout.JobSucceeded, baseTime.Add(-300*time.Hour)),
				manyJobs(10, 20, lookout.JobSucceeded, baseTime.Add(-200*time.Hour)),
				manyJobs(20, 50, lookout.JobSucceeded, baseTime.Add(-(100*time.Hour+5*time.Minute))),
				manyJobs(50, 100, lookout.JobSucceeded, baseTime.Add(-99*time.Hour)),
			),
			jobIdsLeft: sampleJobIds[50:],
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				converter := instructions.NewInstructionConverter(metrics.Get(), "armadaproject.io/", &compress.NoOpCompressor{})
				store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10)

				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Minute)
				defer cancel()
				for _, tj := range tc.jobs {
					storeJob(tj, store, converter)
				}

				dbConn, err := db.Acquire(ctx)
				assert.NoError(t, err)
				err = PruneDb(ctx, dbConn.Conn(), tc.expireAfter, 0, 10, clock.NewFakeClock(baseTime))
				assert.NoError(t, err)

				queriedJobIdsPerTable := []map[string]bool{
					selectStringSet(t, db, "SELECT job_id FROM job"),
					selectStringSet(t, db, "SELECT DISTINCT job_id FROM job_run"),
				}
				for _, queriedJobs := range queriedJobIdsPerTable {
					assert.Equal(t, len(tc.jobIdsLeft), len(queriedJobs))
					for _, jobId := range tc.jobIdsLeft {
						_, ok := queriedJobs[jobId]
						assert.True(t, ok)
					}
				}
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func storeJob(job testJob, db *lookoutdb.LookoutDb, converter *instructions.InstructionConverter) {
	runId := uuid.NewString()
	simulator := repository.NewJobSimulator(converter, db).
		Submit("queue", "jobSet", "owner", "namespace", job.ts, &repository.JobOptions{
			JobId: job.jobId,
			Annotations: map[string]string{
				"armadaproject.io/test-1": "one",
				"armadaproject.io/test-2": "two",
			},
		}).
		Lease(runId, "cluster", "node", job.ts).
		Pending(runId, "cluster", job.ts).
		Running(runId, "node", job.ts)

	switch job.state {
	case lookout.JobSucceeded:
		simulator.
			RunSucceeded(runId, job.ts).
			Succeeded(job.ts).
			Build()
	case lookout.JobFailed:
		simulator.
			RunFailed(runId, "node", 1, "", "", job.ts).
			Failed("node", 1, "", job.ts).
			Build()
	case lookout.JobCancelled:
		simulator.
			Cancelled(job.ts).
			Build()
	case lookout.JobPreempted:
		simulator.
			Preempted(job.ts).
			Build()
	case lookout.JobRunning:
		simulator.
			Build()
	default:
		panic(fmt.Sprintf("job state %s not supported", job.state))
	}
}

func selectStringSet(t *testing.T, db *pgxpool.Pool, query string) map[string]bool {
	t.Helper()
	rows, err := db.Query(armadacontext.TODO(), query)
	assert.NoError(t, err)
	var ss []string
	for rows.Next() {
		var s string
		err := rows.Scan(&s)
		assert.NoError(t, err)
		ss = append(ss, s)
	}
	return util.StringListToSet(ss)
}

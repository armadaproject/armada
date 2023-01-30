package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutv2/repository"
)

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

func TestPruneDb(t *testing.T) {
	type testJob struct {
		jobId string
		ts    time.Time
	}

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

	manyJobs := func(startIdx, endIdx int, ts time.Time) []testJob {
		var testJobs []testJob
		for i := startIdx; i < endIdx; i++ {
			testJobs = append(testJobs, testJob{
				jobId: sampleJobIds[i],
				ts:    ts,
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
				{
					jobId: sampleJobIds[0],
					ts:    baseTime,
				},
				{
					jobId: sampleJobIds[1],
					ts:    baseTime.Add(-9 * time.Hour),
				},
			},
			jobIdsLeft: []string{sampleJobIds[0], sampleJobIds[1]},
		},
		{
			testName:    "expire a job",
			expireAfter: 10 * time.Hour,
			jobs: []testJob{
				{
					jobId: sampleJobIds[0],
					ts:    baseTime,
				},
				{
					jobId: sampleJobIds[1],
					ts:    baseTime.Add(-9 * time.Hour),
				},
				{
					jobId: sampleJobIds[2],
					ts:    baseTime.Add(-(10*time.Hour + 1*time.Minute)),
				},
			},
			jobIdsLeft: []string{sampleJobIds[0], sampleJobIds[1]},
		},
		{
			testName:    "expire many jobs",
			expireAfter: 100 * time.Hour,
			jobs: util.Concat([][]testJob{
				manyJobs(0, 10, baseTime.Add(-300*time.Hour)),
				manyJobs(10, 20, baseTime.Add(-200*time.Hour)),
				manyJobs(20, 50, baseTime.Add(-(100*time.Hour + 5*time.Minute))),
				manyJobs(50, 100, baseTime.Add(-99*time.Hour)),
			}),
			jobIdsLeft: sampleJobIds[50:],
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				converter := instructions.NewInstructionConverter(metrics.Get(), "armadaproject.io/", &compress.NoOpCompressor{})
				store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

				ctx := context.TODO()
				for _, tj := range tc.jobs {
					runId := uuid.NewString()
					repository.NewJobSimulator(converter, store).
						Submit("queue", "jobSet", "owner", tj.ts, &repository.JobOptions{
							JobId: tj.jobId,
							Annotations: map[string]string{
								"armadaproject.io/test-1": "one",
								"armadaproject.io/test-2": "two",
							},
						}).
						Pending(runId, "cluster", tj.ts).
						Running(runId, "node", tj.ts).
						RunSucceeded(runId, tj.ts).
						Succeeded(tj.ts).
						Build()
				}

				dbConn, err := db.Acquire(ctx)
				assert.NoError(t, err)
				err = PruneDb(ctx, dbConn.Conn(), tc.expireAfter, 10, clock.NewFakeClock(baseTime))
				assert.NoError(t, err)

				queriedJobIdsPerTable := []map[string]bool{
					selectStringSet(t, db, "SELECT job_id FROM job"),
					selectStringSet(t, db, "SELECT DISTINCT job_id FROM job_run"),
					selectStringSet(t, db, "SELECT DISTINCT job_id FROM user_annotation_lookup"),
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

func selectStringSet(t *testing.T, db *pgxpool.Pool, query string) map[string]bool {
	t.Helper()
	rows, err := db.Query(context.TODO(), query)
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

package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/broadside/db"
	"github.com/armadaproject/armada/pkg/api"
)

func validJobSpecBytes(t *testing.T) []byte {
	t.Helper()
	job := &api.Job{
		PodSpecs: []*v1.PodSpec{
			{Containers: []v1.Container{{Name: "test", Image: "alpine:latest"}}},
		},
	}
	b, err := proto.Marshal(job)
	require.NoError(t, err)
	return b
}

func TestMemoryDatabase_PopulateHistoricalJobs_JobCount(t *testing.T) {
	m := db.NewMemoryDatabase()
	require.NoError(t, m.InitialiseSchema(context.Background()))

	params := db.HistoricalJobsParams{
		QueueIdx:           0,
		JobSetIdx:          0,
		QueueName:          "test-queue",
		JobSetName:         "test-jobset",
		NJobs:              100,
		SucceededThreshold: 800,
		ErroredThreshold:   900,
		CancelledThreshold: 950,
		JobAgeDays:         []int{0},
		JobSpecBytes:       validJobSpecBytes(t),
		ErrorBytes:         []byte("simulated error"),
		DebugBytes:         []byte("debug msg"),
		PreemptionBytes:    []byte("preempted"),
	}

	err := m.PopulateHistoricalJobs(context.Background(), params)
	require.NoError(t, err)

	ctx := context.Background()
	jobs, err := m.GetJobs(&ctx, nil, false, nil, 0, 1000)
	require.NoError(t, err)
	assert.Len(t, jobs, 100, "should have inserted exactly 100 jobs")
}

func TestMemoryDatabase_PopulateHistoricalJobs_StateDistribution(t *testing.T) {
	m := db.NewMemoryDatabase()
	require.NoError(t, m.InitialiseSchema(context.Background()))

	// 70% succeeded, 10% errored, 10% cancelled, 10% preempted
	params := db.HistoricalJobsParams{
		QueueIdx:           0,
		JobSetIdx:          0,
		QueueName:          "test-queue",
		JobSetName:         "test-jobset",
		NJobs:              1000,
		SucceededThreshold: 700,
		ErroredThreshold:   800,
		CancelledThreshold: 900,
		JobAgeDays:         []int{0},
		JobSpecBytes:       validJobSpecBytes(t),
		ErrorBytes:         []byte("simulated error"),
		DebugBytes:         []byte("debug msg"),
		PreemptionBytes:    []byte("preempted"),
	}

	err := m.PopulateHistoricalJobs(context.Background(), params)
	require.NoError(t, err)

	ctx := context.Background()
	jobs, err := m.GetJobs(&ctx, nil, false, nil, 0, 2000)
	require.NoError(t, err)

	states := make(map[string]int)
	for _, j := range jobs {
		states[j.State]++
	}

	assert.Equal(t, 700, states["SUCCEEDED"])
	assert.Equal(t, 100, states["FAILED"])
	assert.Equal(t, 100, states["CANCELLED"])
	assert.Equal(t, 100, states["PREEMPTED"])
}

func TestMemoryDatabase_PopulateHistoricalJobs_UniqueJobIDs(t *testing.T) {
	m := db.NewMemoryDatabase()
	require.NoError(t, m.InitialiseSchema(context.Background()))

	params := db.HistoricalJobsParams{
		QueueIdx:           1,
		JobSetIdx:          2,
		QueueName:          "queue-a",
		JobSetName:         "jobset-b",
		NJobs:              500,
		SucceededThreshold: 1000,
		ErroredThreshold:   1000,
		CancelledThreshold: 1000,
		JobAgeDays:         []int{0},
		JobSpecBytes:       validJobSpecBytes(t),
		ErrorBytes:         []byte("err"),
		DebugBytes:         []byte("dbg"),
		PreemptionBytes:    []byte("pre"),
	}

	err := m.PopulateHistoricalJobs(context.Background(), params)
	require.NoError(t, err)

	ctx := context.Background()
	jobs, err := m.GetJobs(&ctx, nil, false, nil, 0, 1000)
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for _, j := range jobs {
		_, dup := seen[j.JobId]
		assert.False(t, dup, "duplicate job ID: %s", j.JobId)
		seen[j.JobId] = struct{}{}
	}
}

func TestCalculateRuntimeNegativeFinishFloorsRuntime(t *testing.T) {
	m := db.NewMemoryDatabase()
	require.NoError(t, m.InitialiseSchema(context.Background()))

	const jobID = "123456789012345678"
	const runID = jobID + "-0"
	started := time.Date(2026, time.January, 1, 0, 1, 0, 0, time.UTC)
	finished := started.Add(-time.Minute)
	queries := []db.IngestionQuery{
		db.InsertJob{Job: &db.NewJob{JobID: jobID, Queue: "queue", JobSet: "job-set", Owner: "owner", Submitted: finished}},
		db.InsertJobRun{JobRunID: runID, JobID: jobID, Time: finished},
		db.SetJobRunning{JobID: jobID, Time: started, LatestRunID: runID, Submitted: finished},
		db.SetJobRunStarted{JobRunID: runID, Time: started},
		db.SetJobRunSucceeded{JobRunID: runID, Time: finished},
	}
	require.NoError(t, m.ExecuteIngestionQueryBatch(context.Background(), queries))

	ctx := context.Background()
	jobs, err := m.GetJobs(&ctx, nil, false, nil, 0, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, int32(0), jobs[0].RuntimeSeconds)
}

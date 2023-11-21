package jobdb

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func NewTestJobDb() *JobDb {
	return NewJobDb(
		map[string]types.PriorityClass{
			"foo": {},
			"bar": {},
		},
		"foo",
	)
}

func TestJobDb_TestUpsert(t *testing.T) {
	jobDb := NewTestJobDb()

	job1 := newJob()
	job2 := newJob()
	txn := jobDb.WriteTxn()

	// Insert Job
	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	retrieved := txn.GetById(job1.Id())
	assert.Equal(t, job1, retrieved)
	retrieved = txn.GetById(job2.Id())
	assert.Equal(t, job2, retrieved)

	// Updated Job
	job1Updated := job1.WithQueued(true)
	err = txn.Upsert([]*Job{job1Updated})
	require.NoError(t, err)
	retrieved = txn.GetById(job1.Id())
	assert.Equal(t, job1Updated, retrieved)

	// Can't insert with read only transaction
	err = (jobDb.ReadTxn()).Upsert([]*Job{job1})
	require.Error(t, err)
}

func TestJobDb_TestGetById(t *testing.T) {
	jobDb := NewTestJobDb()
	job1 := newJob()
	job2 := newJob()
	txn := jobDb.WriteTxn()

	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	assert.Equal(t, job1, txn.GetById(job1.Id()))
	assert.Equal(t, job2, txn.GetById(job2.Id()))
	assert.Nil(t, txn.GetById(util.NewULID()))
}

func TestJobDb_TestGetByRunId(t *testing.T) {
	jobDb := NewTestJobDb()
	job1 := newJob().WithNewRun("executor", "nodeId", "nodeName")
	job2 := newJob().WithNewRun("executor", "nodeId", "nodeName")
	txn := jobDb.WriteTxn()

	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	assert.Equal(t, job1, txn.GetByRunId(job1.LatestRun().id))
	assert.Equal(t, job2, txn.GetByRunId(job2.LatestRun().id))
	assert.Nil(t, txn.GetByRunId(uuid.New()))

	err = txn.BatchDelete([]string{job1.Id()})
	require.NoError(t, err)
	assert.Nil(t, txn.GetByRunId(job1.LatestRun().id))
}

func TestJobDb_TestHasQueuedJobs(t *testing.T) {
	jobDb := NewTestJobDb()
	job1 := newJob().WithNewRun("executor", "nodeId", "nodeName")
	job2 := newJob().WithNewRun("executor", "nodeId", "nodeName")
	txn := jobDb.WriteTxn()

	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	assert.False(t, txn.HasQueuedJobs(job1.queue))
	assert.False(t, txn.HasQueuedJobs("non-existent-queue"))

	err = txn.Upsert([]*Job{job1.WithQueued(true)})
	require.NoError(t, err)
	assert.True(t, txn.HasQueuedJobs(job1.queue))
	assert.False(t, txn.HasQueuedJobs("non-existent-queue"))
}

func TestJobDb_TestQueuedJobs(t *testing.T) {
	jobDb := NewTestJobDb()
	jobs := make([]*Job, 10)
	for i := 0; i < len(jobs); i++ {
		jobs[i] = newJob().WithQueued(true)
		jobs[i].priority = 1000
		jobs[i].submittedTime = int64(i) // Ensures jobs are ordered.
	}
	shuffledJobs := slices.Clone(jobs)
	rand.Shuffle(len(shuffledJobs), func(i, j int) { shuffledJobs[i], shuffledJobs[j] = shuffledJobs[j], jobs[i] })
	txn := jobDb.WriteTxn()

	err := txn.Upsert(jobs)
	require.NoError(t, err)
	collect := func() []*Job {
		retrieved := make([]*Job, 0)
		iter := txn.QueuedJobs(jobs[0].GetQueue())
		for !iter.Done() {
			j, _ := iter.Next()
			retrieved = append(retrieved, j)
		}
		return retrieved
	}

	assert.Equal(t, jobs, collect())

	// remove some jobs
	err = txn.BatchDelete([]string{jobs[1].id, jobs[3].id, jobs[5].id})
	require.NoError(t, err)
	assert.Equal(t, []*Job{jobs[0], jobs[2], jobs[4], jobs[6], jobs[7], jobs[8], jobs[9]}, collect())

	// dequeue some jobs
	err = txn.Upsert([]*Job{jobs[7].WithQueued(false), jobs[4].WithQueued(false)})
	require.NoError(t, err)
	assert.Equal(t, []*Job{jobs[0], jobs[2], jobs[6], jobs[8], jobs[9]}, collect())

	// change the priority of a job to put it to the front of the queue
	updatedJob := jobs[8].WithPriority(0)
	err = txn.Upsert([]*Job{updatedJob})
	require.NoError(t, err)
	assert.Equal(t, []*Job{updatedJob, jobs[0], jobs[2], jobs[6], jobs[9]}, collect())

	// new job
	job10 := newJob().WithPriority(90).WithQueued(true)
	err = txn.Upsert([]*Job{job10})
	require.NoError(t, err)
	assert.Equal(t, []*Job{updatedJob, job10, jobs[0], jobs[2], jobs[6], jobs[9]}, collect())

	// clear all jobs
	err = txn.BatchDelete([]string{updatedJob.id, job10.id, jobs[0].id, jobs[2].id, jobs[6].id, jobs[9].id})
	require.NoError(t, err)
	assert.Equal(t, []*Job{}, collect())
}

func TestJobDb_TestGetAll(t *testing.T) {
	jobDb := NewTestJobDb()
	job1 := newJob().WithNewRun("executor", "nodeId", "nodeName")
	job2 := newJob().WithNewRun("executor", "nodeId", "nodeName")
	txn := jobDb.WriteTxn()
	assert.Equal(t, []*Job{}, txn.GetAll())

	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	actual := txn.GetAll()
	expected := []*Job{job1, job2}
	slices.SortFunc(expected, func(a, b *Job) bool {
		return a.id > b.id
	})
	slices.SortFunc(actual, func(a, b *Job) bool {
		return a.id > b.id
	})
	assert.Equal(t, expected, actual)
}

func TestJobDb_TestTransactions(t *testing.T) {
	jobDb := NewTestJobDb()
	job := newJob()

	txn1 := jobDb.WriteTxn()
	txn2 := jobDb.ReadTxn()
	err := txn1.Upsert([]*Job{job})
	require.NoError(t, err)

	assert.NotNil(t, txn1.GetById(job.id))
	assert.Nil(t, txn2.GetById(job.id))
	txn1.Commit()

	txn3 := jobDb.ReadTxn()
	assert.NotNil(t, txn3.GetById(job.id))

	assert.Error(t, txn1.Upsert([]*Job{job})) // should be error as you can't insert after committing
}

func TestJobDb_TestBatchDelete(t *testing.T) {
	jobDb := NewTestJobDb()
	job1 := newJob().WithQueued(true).WithNewRun("executor", "nodeId", "nodeName")
	job2 := newJob().WithQueued(true).WithNewRun("executor", "nodeId", "nodeName")
	txn := jobDb.WriteTxn()

	// Insert Job
	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	err = txn.BatchDelete([]string{job2.Id()})
	require.NoError(t, err)
	assert.NotNil(t, txn.GetById(job1.Id()))
	assert.Nil(t, txn.GetById(job2.Id()))

	// Can't delete with read only transaction
	err = (jobDb.ReadTxn()).BatchDelete([]string{job1.Id()})
	require.Error(t, err)
}

func TestJobDb_SchedulingKey(t *testing.T) {
	podRequirements := &schedulerobjects.PodRequirements{
		NodeSelector: map[string]string{"foo": "bar"},
		Priority:     2,
	}
	jobSchedulingInfo := &schedulerobjects.JobSchedulingInfo{
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: podRequirements,
				},
			},
		},
	}
	jobDb := NewTestJobDb()
	job := jobDb.NewJob("jobId", "jobSet", "queue", 1, jobSchedulingInfo, false, 0, false, false, false, 2)
	assert.Equal(t,
		jobDb.schedulingKeyGenerator.Key(
			podRequirements.NodeSelector,
			podRequirements.Affinity,
			podRequirements.Tolerations,
			podRequirements.ResourceRequirements.Requests,
			podRequirements.Priority,
		),
		job.schedulingKey,
	)
}

func newJob() *Job {
	return &Job{
		id:                util.NewULID(),
		queue:             "test-queue",
		priority:          0,
		submittedTime:     0,
		queued:            false,
		runsById:          map[uuid.UUID]*JobRun{},
		jobSchedulingInfo: schedulingInfo,
	}
}

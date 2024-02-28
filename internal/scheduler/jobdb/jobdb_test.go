package jobdb

import (
	"math/rand"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func NewTestJobDb() *JobDb {
	return NewJobDb(
		map[string]types.PriorityClass{
			"foo": {},
			"bar": {},
		},
		"foo",
		1024,
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
	job1 := newJob().WithNewRun("executor", "nodeId", "nodeName", 5)
	job2 := newJob().WithNewRun("executor", "nodeId", "nodeName", 10)
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
	job1 := newJob().WithNewRun("executor", "nodeId", "nodeName", 5)
	job2 := newJob().WithNewRun("executor", "nodeId", "nodeName", 10)
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
	job1 := newJob().WithNewRun("executor", "nodeId", "nodeName", 5)
	job2 := newJob().WithNewRun("executor", "nodeId", "nodeName", 10)
	txn := jobDb.WriteTxn()
	assert.Equal(t, []*Job{}, txn.GetAll())

	err := txn.Upsert([]*Job{job1, job2})
	require.NoError(t, err)
	actual := txn.GetAll()
	expected := []*Job{job1, job2}
	slices.SortFunc(expected, func(a, b *Job) int {
		if a.id > b.id {
			return -1
		} else if a.id < b.id {
			return 1
		} else {
			return 0
		}
	})
	slices.SortFunc(actual, func(a, b *Job) int {
		if a.id > b.id {
			return -1
		} else if a.id < b.id {
			return 1
		} else {
			return 0
		}
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
	job1 := newJob().WithQueued(true).WithNewRun("executor", "nodeId", "nodeName", 5)
	job2 := newJob().WithQueued(true).WithNewRun("executor", "nodeId", "nodeName", 10)
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

func TestJobDb_SchedulingKeyIsPopulated(t *testing.T) {
	podRequirements := &schedulerobjects.PodRequirements{
		NodeSelector: map[string]string{"foo": "bar"},
		Priority:     2,
	}
	jobSchedulingInfo := &schedulerobjects.JobSchedulingInfo{
		PriorityClassName: "foo",
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

	actualSchedulingKey, ok := job.GetSchedulingKey()
	require.True(t, ok)
	assert.Equal(t, interfaces.SchedulingKeyFromLegacySchedulerJob(jobDb.schedulingKeyGenerator, job), actualSchedulingKey)
}

func TestJobDb_SchedulingKey(t *testing.T) {
	tests := map[string]struct {
		podRequirementsA   *schedulerobjects.PodRequirements
		priorityClassNameA string
		podRequirementsB   *schedulerobjects.PodRequirements
		priorityClassNameB string
		equal              bool
	}{
		"annotations does not affect key": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Annotations: map[string]string{
					"foo":  "bar",
					"fish": "chips",
					"salt": "pepper",
				},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Annotations: map[string]string{
					"foo":  "bar",
					"fish": "chips",
				},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			equal: true,
		},
		"preemptionPolicy does not affect key": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abcdef",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			equal: true,
		},
		"limits does not affect key": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abcdef",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("4"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			equal: true,
		},
		"priority": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 2,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			equal: true,
		},
		"zero request does not affect key": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
						"foo":    resource.MustParse("0"),
					},
				},
			},
			equal: true,
		},
		"nodeSelector key": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
					"property2": "value2",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"nodeSelector value": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1-2",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"nodeSelector different keys, same values": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"my-cool-label": "value",
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"my-other-cool-label": "value",
				},
			},
			equal: false,
		},
		"toleration key": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a-2",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration operator": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b-2",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration value": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b-2",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration effect": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d-2",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration tolerationSeconds": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(2),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			equal: true,
		},
		"key ordering": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
						"cpu":    resource.MustParse("4"),
					},
				},
			},
			equal: true,
		},
		"affinity PodAffinity ignored": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
					PodAffinity: &v1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label1": "labelval1",
										"label2": "labelval2",
										"label3": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2", "v3"},
										},
									},
								},
								Namespaces:  []string{"n1, n2, n3"},
								TopologyKey: "topkey1",
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label10": "labelval1",
										"label20": "labelval2",
										"label30": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k10",
											Operator: "o10",
											Values:   []string{"v10", "v20", "v30"},
										},
									},
								},
							},
						},
					},
					PodAntiAffinity: nil,
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
					PodAffinity: &v1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label1": "labelval1-2",
										"label2": "labelval2",
										"label3": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2", "v3"},
										},
									},
								},
								Namespaces:  []string{"n1, n2, n3"},
								TopologyKey: "topkey1",
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label10": "labelval1",
										"label20": "labelval2",
										"label30": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k10",
											Operator: "o10",
											Values:   []string{"v10", "v20", "v30"},
										},
									},
								},
							},
						},
					},
					PodAntiAffinity: nil,
				},
			},
			equal: true,
		},
		"affinity NodeAffinity MatchExpressions": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v3"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
				},
			},
			equal: false,
		},
		"affinity NodeAffinity MatchFields": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v21"},
										},
									},
								},
							},
						},
					},
				},
			},
			equal: false,
		},
		"affinity NodeAffinity multiple MatchFields": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
				},
			},
			podRequirementsB: &schedulerobjects.PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
										{
											Key:      "k3",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
				},
			},
			equal: false,
		},
		"priority class names equal": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")},
				},
			},
			priorityClassNameA: "my-cool-priority-class",
			podRequirementsB: &schedulerobjects.PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")},
				},
			},
			priorityClassNameB: "my-cool-priority-class",
			equal:              true,
		},
		"priority class names different": {
			podRequirementsA: &schedulerobjects.PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")},
				},
			},
			priorityClassNameA: "my-cool-priority-class",
			podRequirementsB: &schedulerobjects.PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")},
				},
			},
			priorityClassNameB: "my-cool-other-priority-class",
			equal:              false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			skg := schedulerobjects.NewSchedulingKeyGenerator()

			jobSchedulingInfoA := proto.Clone(jobSchedulingInfo).(*schedulerobjects.JobSchedulingInfo)
			jobSchedulingInfoA.PriorityClassName = tc.priorityClassNameA
			jobSchedulingInfoA.ObjectRequirements[0].Requirements = &schedulerobjects.ObjectRequirements_PodRequirements{PodRequirements: tc.podRequirementsA}
			jobA := baseJob.WithJobSchedulingInfo(jobSchedulingInfoA)

			jobSchedulingInfoB := proto.Clone(jobSchedulingInfo).(*schedulerobjects.JobSchedulingInfo)
			jobSchedulingInfoB.PriorityClassName = tc.priorityClassNameB
			jobSchedulingInfoB.ObjectRequirements[0].Requirements = &schedulerobjects.ObjectRequirements_PodRequirements{PodRequirements: tc.podRequirementsB}
			jobB := baseJob.WithJobSchedulingInfo(jobSchedulingInfoB)

			schedulingKeyA := interfaces.SchedulingKeyFromLegacySchedulerJob(skg, jobA)
			schedulingKeyB := interfaces.SchedulingKeyFromLegacySchedulerJob(skg, jobB)

			// Generate the keys several times to check their consistency.
			for i := 1; i < 10; i++ {
				assert.Equal(t, interfaces.SchedulingKeyFromLegacySchedulerJob(skg, jobA), schedulingKeyA)
				assert.Equal(t, interfaces.SchedulingKeyFromLegacySchedulerJob(skg, jobB), schedulingKeyB)
			}

			if tc.equal {
				assert.Equal(t, schedulingKeyA, schedulingKeyB)
			} else {
				assert.NotEqual(t, schedulingKeyA, schedulingKeyB)
			}
		})
	}
}

func newJob() *Job {
	return &Job{
		jobDb:             NewTestJobDb(),
		id:                util.NewULID(),
		queue:             "test-queue",
		priority:          0,
		submittedTime:     0,
		queued:            false,
		runsById:          map[uuid.UUID]*JobRun{},
		jobSchedulingInfo: jobSchedulingInfo,
	}
}

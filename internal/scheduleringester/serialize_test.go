package scheduleringester

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/pulsarutils"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

// allDbOperations returns one instance of every concrete DbOperation type, keyed by a
// human-readable name. TestAllDbOperationsCovered guards this set against silently going stale
// as DbOperation types are added or removed in dbops.go.
func allDbOperations() map[string]DbOperation {
	return map[string]DbOperation{
		"InsertJobs":             InsertJobs{"job1": &JobInsertion{}},
		"InsertRuns":             InsertRuns{"run1": &JobRunDetails{}},
		"UpdateJobSetPriorities": UpdateJobSetPriorities{{queue: "queue1", jobSet: "set1"}: 1},
		"MarkJobSetsCancelRequested": MarkJobSetsCancelRequested{
			cancelUser:   "user1",
			cancelReason: "reason1",
			jobSets:      map[JobSetKey]*JobSetCancelAction{{queue: "queue1", jobSet: "set1"}: {cancelQueued: true}},
		},
		"MarkJobsCancelRequested": MarkJobsCancelRequested{
			cancelUser:   "user1",
			cancelReason: "reason1",
			jobIds:       map[JobSetKey][]string{{queue: "queue1", jobSet: "set1"}: {"job1"}},
		},
		"MarkJobsCancelled":              MarkJobsCancelled{"job1": time.Now()},
		"MarkJobsSucceeded":              MarkJobsSucceeded{"job1": true},
		"MarkJobsFailed":                 MarkJobsFailed{"job1": true},
		"UpdateJobSchedulingInfo":        UpdateJobSchedulingInfo{"job1": &JobSchedulingInfoUpdate{}},
		"UpdateJobQueuedState":           UpdateJobQueuedState{"job1": &JobQueuedStateUpdate{}},
		"MarkRunsSucceeded":              MarkRunsSucceeded{"run1": time.Now()},
		"MarkRunsFailed":                 MarkRunsFailed{"run1": &JobRunFailed{}},
		"MarkRunsForJobPreemptRequested": MarkRunsForJobPreemptRequested{{queue: "queue1", jobSet: "set1"}: {"job1": "run1"}},
		"MarkRunsRunning":                MarkRunsRunning{"run1": time.Now()},
		"MarkRunsPending":                MarkRunsPending{"run1": time.Now()},
		"MarkRunsPreempted":              MarkRunsPreempted{"run1": time.Now()},
		"InsertJobRunErrors":             InsertJobRunErrors{"run1": &schedulerdb.JobRunError{}},
		"UpdateJobPriorities": &UpdateJobPriorities{
			key:    JobReprioritiseKey{JobSetKey: JobSetKey{queue: "queue1", jobSet: "set1"}, Priority: 1},
			jobIds: []string{"job1"},
		},
		"MarkJobsValidated":     MarkJobsValidated{"job1": {"pool1"}},
		"InsertPartitionMarker": &InsertPartitionMarker{markers: []*schedulerdb.Marker{{}}},
		"UpsertExecutorSettings": UpsertExecutorSettings{
			"executor1": {ExecutorID: "executor1", Cordoned: true},
		},
		"DeleteExecutorSettings": DeleteExecutorSettings{"executor1": {ExecutorID: "executor1"}},
		"PreemptExecutor":        PreemptExecutor{"executor1": {Name: "executor1"}},
		"CancelExecutor":         CancelExecutor{"executor1": {Name: "executor1"}},
		"PreemptNode":            PreemptNode{{Node: "node1", Executor: "executor1"}: {Name: "node1"}},
		"CancelNode":             CancelNode{{Node: "node1", Executor: "executor1"}: {Name: "node1"}},
		"PreemptQueue":           PreemptQueue{"queue1": {Name: "queue1"}},
		"CancelQueue":            CancelQueue{"queue1": {Name: "queue1", JobStates: []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_QUEUED}}},
	}
}

// TestAllDbOperationsCovered guards against allDbOperations silently going stale: it fails if
// the number of concrete types it constructs no longer matches the number of DbOperation
// implementations, which would happen if a DbOperation type were added to dbops.go without
// also being added here.
func TestAllDbOperationsCovered(t *testing.T) {
	assert.Len(t, allDbOperations(), 28)
}

// TestSerializeForDLQ_NeverEmpty asserts that every current concrete DbOperation type's
// SerializeForDLQ implementation produces non-empty JSON output.
func TestSerializeForDLQ_NeverEmpty(t *testing.T) {
	for name, op := range allDbOperations() {
		t.Run(name, func(t *testing.T) {
			data := op.SerializeForDLQ()
			bytes, err := json.Marshal(data)
			assert.NoError(t, err)
			assert.NotEqual(t, "{}", string(bytes), "SerializeForDLQ produced empty output for %s", name)
		})
	}
}

// TestSerialize_UnexportedFieldsSurface confirms that DbOperation types whose data lives in
// unexported struct fields (and so does not appear in the source struct's JSON output) is
// nonetheless surfaced by SerializeForDLQ.
func TestSerialize_UnexportedFieldsSurface(t *testing.T) {
	db := &SchedulerDb{}

	instructions := &DbOperationsWithMessageIds{
		Ops: []DbOperation{
			MarkJobSetsCancelRequested{
				cancelUser:   "user1",
				cancelReason: "reason1",
				jobSets:      map[JobSetKey]*JobSetCancelAction{{queue: "queue1", jobSet: "set1"}: {cancelQueued: true}},
			},
			MarkJobsCancelRequested{
				cancelUser:   "user2",
				cancelReason: "reason2",
				jobIds:       map[JobSetKey][]string{{queue: "queue1", jobSet: "set1"}: {"job1"}},
			},
			&UpdateJobPriorities{
				key:    JobReprioritiseKey{JobSetKey: JobSetKey{queue: "queue1", jobSet: "set1"}, Priority: 5},
				jobIds: []string{"job1", "job2"},
			},
			&InsertPartitionMarker{markers: []*schedulerdb.Marker{{GroupID: uuid.New(), PartitionID: 3}}},
		},
	}

	bytes, err := db.Serialize(instructions)
	assert.NoError(t, err)

	s := string(bytes)
	assert.Contains(t, s, "user1")
	assert.Contains(t, s, "reason1")
	assert.Contains(t, s, "user2")
	assert.Contains(t, s, "reason2")
	assert.Contains(t, s, "job1")
	assert.Contains(t, s, "job2")
	assert.Contains(t, s, `"Priority":5`)
	assert.Contains(t, s, `"PartitionID":3`)
}

// TestSerialize_MessageIdsAsStrings confirms MessageIds round-trips as an array of strings,
// not "{}", which would happen if pulsar.MessageID (an interface) were marshalled directly.
func TestSerialize_MessageIdsAsStrings(t *testing.T) {
	db := &SchedulerDb{}
	instructions := &DbOperationsWithMessageIds{
		Ops:        []DbOperation{MarkJobsSucceeded{"job1": true}},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1), pulsarutils.NewMessageId(2)},
	}

	bytes, err := db.Serialize(instructions)
	assert.NoError(t, err)

	var decoded struct {
		MessageIds []string
	}
	assert.NoError(t, json.Unmarshal(bytes, &decoded))
	assert.Equal(t, []string{
		pulsarutils.NewMessageId(1).String(),
		pulsarutils.NewMessageId(2).String(),
	}, decoded.MessageIds)
}

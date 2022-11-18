package scheduleringester

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common/ingest/metrics"
	f "github.com/G-Research/armada/internal/common/ingest/testfixtures"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/internal/scheduler/sqlc"
	"github.com/G-Research/armada/pkg/armadaevents"
)

var m = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "test_")

func TestConvertSequence(t *testing.T) {
	tests := map[string]struct {
		events   []*armadaevents.EventSequence_Event
		filter   func(event *armadaevents.EventSequence_Event) bool
		expected []DbOperation
	}{
		"submit": {
			events: []*armadaevents.EventSequence_Event{f.Submit},
			expected: []DbOperation{InsertJobs{f.JobIdUuid: &sqlc.Job{
				JobID:         f.JobIdUuid,
				JobSet:        f.JobSetName,
				UserID:        f.UserId,
				Groups:        f.Groups,
				Queue:         f.Queue,
				Priority:      int64(f.Priority),
				SubmitMessage: mustMarshall(f.Submit.GetSubmitJob()),
				SchedulingInfo: mustMarshall(&schedulerobjects.JobSchedulingInfo{
					Lifetime:        0,
					AtMostOnce:      true,
					Preemptible:     true,
					ConcurrencySafe: true,
					ObjectRequirements: []*schedulerobjects.ObjectRequirements{
						{
							Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
								PodRequirements: &schedulerobjects.PodRequirements{
									NodeSelector:     f.NodeSelector,
									Tolerations:      f.Tolerations,
									PreemptionPolicy: "PreemptLowerPriority",
									ResourceRequirements: v1.ResourceRequirements{
										Limits: map[v1.ResourceName]resource.Quantity{
											"memory": resource.MustParse("64Mi"),
											"cpu":    resource.MustParse("150m"),
										},
										Requests: map[v1.ResourceName]resource.Quantity{
											"memory": resource.MustParse("64Mi"),
											"cpu":    resource.MustParse("150m"),
										},
									},
								},
							},
						},
					},
				}),
			}}},
		},
		"job run assigned": {
			events: []*armadaevents.EventSequence_Event{f.Assigned},
			expected: []DbOperation{InsertRunAssignments{f.RunIdUuid: &sqlc.JobRunAssignment{
				RunID:      f.RunIdUuid,
				Assignment: mustMarshall(f.Assigned.GetJobRunAssigned()),
			}}},
		},
		"job run leased": {
			events: []*armadaevents.EventSequence_Event{f.Leased},
			expected: []DbOperation{InsertRuns{f.RunIdUuid: &sqlc.Run{
				RunID:    f.RunIdUuid,
				JobID:    f.JobIdUuid,
				JobSet:   f.JobSetName,
				Executor: f.ExecutorId,
			}}},
		},
		"job run running": {
			events:   []*armadaevents.EventSequence_Event{f.Running},
			expected: []DbOperation{MarkRunsRunning{f.RunIdUuid: true}},
		},
		"job run succeeded": {
			events:   []*armadaevents.EventSequence_Event{f.JobRunSucceeded},
			expected: []DbOperation{MarkRunsSucceeded{f.RunIdUuid: true}},
		},
		// TODO: can we remove the error message from the fields stored
		// If so we can have the simple test below
		//"job run errors terminal": {
		//	events: []*armadaevents.EventSequence_Event{f.LeaseReturned},
		//	expected: []DbOperation{
		//		MarkJobsFailed{f.RunIdUuid: true},
		//	},
		//},
		"job succeeded": {
			events: []*armadaevents.EventSequence_Event{f.JobSucceeded},
			expected: []DbOperation{
				MarkJobsSucceeded{f.JobIdUuid: true},
			},
		},
		"reprioritise job": {
			events: []*armadaevents.EventSequence_Event{f.JobReprioritiseRequested},
			expected: []DbOperation{
				UpdateJobPriorities{f.JobIdUuid: f.NewPriority},
			},
		},
		"reprioritise jobset": {
			events: []*armadaevents.EventSequence_Event{f.JobSetReprioritiseRequested},
			expected: []DbOperation{
				UpdateJobSetPriorities{f.JobSetName: f.NewPriority},
			},
		},
		"cancel job": {
			events: []*armadaevents.EventSequence_Event{f.JobCancelRequested},
			expected: []DbOperation{
				MarkJobsCancelled{f.JobIdUuid: true},
			},
		},
		"cancel jobSet": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequested},
			expected: []DbOperation{
				MarkJobSetsCancelled{f.JobSetName: true},
			},
		},
		"multiple events": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequested, f.Running, f.JobSucceeded},
			expected: []DbOperation{
				MarkJobSetsCancelled{f.JobSetName: true},
				MarkRunsRunning{f.RunIdUuid: true},
				MarkJobsSucceeded{f.JobIdUuid: true},
			},
		},
		"filtered events": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequested, f.Running, f.JobSucceeded},
			filter: func(event *armadaevents.EventSequence_Event) bool {
				return event.GetJobRunRunning() != nil
			},
			expected: []DbOperation{MarkRunsRunning{f.RunIdUuid: true}},
		},
		"ignored events": {
			events: []*armadaevents.EventSequence_Event{f.Running, f.JobCancelled, f.JobSucceeded},
			expected: []DbOperation{
				MarkRunsRunning{f.RunIdUuid: true},
				MarkJobsSucceeded{f.JobIdUuid: true},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.filter == nil {
				tc.filter = func(event *armadaevents.EventSequence_Event) bool {
					return true
				}
			}
			converter := InstructionConverter{m, tc.filter}
			es := f.NewEventSequence(tc.events...)
			results := converter.convertSequence(es)
			assertOperationsEqual(t, tc.expected, results)
		})
	}
}

func assertOperationsEqual(t *testing.T, expectedOps []DbOperation, actualOps []DbOperation) {
	t.Helper()
	assert.Equal(t, len(expectedOps), len(actualOps), "operations arrays are not the same length")
	for i := 0; i < len(expectedOps); i++ {
		expectedOp := expectedOps[i]
		actualOp := actualOps[i]
		assert.IsType(t, expectedOp, actualOp, fmt.Sprintf("operations at index [%d] are not of equal types", i))
		switch expectedOp.(type) {
		case InsertJobs:
			actualSubmits := actualOp.(InsertJobs)
			for k, expectedSubmit := range expectedOp.(InsertJobs) {
				actualSubmit, ok := actualSubmits[k]
				assert.True(t, ok)
				assertSubmitMessagesEqual(t, expectedSubmit.SubmitMessage, actualSubmit.SubmitMessage)
				assertSchedulingInfoEqual(t, expectedSubmit.SchedulingInfo, actualSubmit.SchedulingInfo)
				// nil out the byte arrays
				actualSubmit.SchedulingInfo = nil
				actualSubmit.SubmitMessage = nil
				expectedSubmit.SchedulingInfo = nil
				expectedSubmit.SubmitMessage = nil
				assert.Equal(t, expectedSubmit, actualSubmit)
			}
		default:
			assert.Equal(t, expectedOp, actualOp)
		}
	}
}

func assertSchedulingInfoEqual(t *testing.T, expectedBytes []byte, actualBytes []byte) {
	actualSchedInfo, err := unmarshalSchedulingInfo(actualBytes)
	assert.NoError(t, err)
	expectedSchedInfo, err := unmarshalSchedulingInfo(expectedBytes)
	assert.NoError(t, err)
	assert.Equal(t, expectedSchedInfo, actualSchedInfo)
}

func assertSubmitMessagesEqual(t *testing.T, expectedBytes []byte, actualBytes []byte) {
	actualSubmitMessage, err := unmarshalSubmitMsg(actualBytes)
	assert.NoError(t, err)
	expectedSubmitMessage, err := unmarshalSubmitMsg(expectedBytes)
	assert.NoError(t, err)
	assert.Equal(t, expectedSubmitMessage, actualSubmitMessage)
}

func unmarshalSubmitMsg(b []byte) (*armadaevents.SubmitJob, error) {
	sm := &armadaevents.SubmitJob{}
	err := proto.Unmarshal(b, sm)
	return sm, err
}

func unmarshalSchedulingInfo(b []byte) (*schedulerobjects.JobSchedulingInfo, error) {
	sm := &schedulerobjects.JobSchedulingInfo{}
	err := proto.Unmarshal(b, sm)
	return sm, err
}

func mustMarshall(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

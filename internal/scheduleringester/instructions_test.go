package scheduleringester

import (
	"fmt"
	"testing"

	"github.com/armadaproject/armada/internal/common/compress"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	f "github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	m                   = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "test_")
	compressedGroups, _ = compress.CompressStringArray(f.Groups, &compress.NoOpCompressor{})
)

func TestConvertSequence(t *testing.T) {
	tests := map[string]struct {
		events   []*armadaevents.EventSequence_Event
		filter   func(event *armadaevents.EventSequence_Event) bool
		expected []DbOperation
	}{
		"submit": {
			events: []*armadaevents.EventSequence_Event{f.Submit},
			expected: []DbOperation{InsertJobs{f.JobIdString: &schedulerdb.Job{
				JobID:         f.JobIdString,
				JobSet:        f.JobSetName,
				UserID:        f.UserId,
				Groups:        compressedGroups,
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
		"job run leased": {
			events: []*armadaevents.EventSequence_Event{f.Leased},
			expected: []DbOperation{InsertRuns{f.RunIdUuid: &schedulerdb.Run{
				RunID:    f.RunIdUuid,
				JobID:    f.JobIdString,
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
		"job run errors terminal": {
			events: []*armadaevents.EventSequence_Event{f.LeaseReturned},
			expected: []DbOperation{
				MarkRunsFailed{f.RunIdUuid: true},
			},
		},
		"job errors terminal": {
			events: []*armadaevents.EventSequence_Event{f.JobFailed},
			expected: []DbOperation{
				MarkJobsFailed{f.JobIdString: true},
			},
		},
		"job succeeded": {
			events: []*armadaevents.EventSequence_Event{f.JobSucceeded},
			expected: []DbOperation{
				MarkJobsSucceeded{f.JobIdString: true},
			},
		},
		"reprioritise job": {
			events: []*armadaevents.EventSequence_Event{f.JobReprioritiseRequested},
			expected: []DbOperation{
				UpdateJobPriorities{f.JobIdString: f.NewPriority},
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
				MarkJobsCancelled{f.JobIdString: true},
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
				MarkJobsSucceeded{f.JobIdString: true},
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
				MarkJobsSucceeded{f.JobIdString: true},
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
			converter := InstructionConverter{m, tc.filter, &compress.NoOpCompressor{}}
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

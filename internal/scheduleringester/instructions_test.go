package scheduleringester

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	f "github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	compressor   = compress.NewThreadSafeZlibCompressor(1024)
	decompressor = compress.NewThreadSafeZlibDecompressor()
	m            = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "test_")
)

func TestConvertSequence(t *testing.T) {
	tests := map[string]struct {
		events   []*armadaevents.EventSequence_Event
		expected []DbOperation
	}{
		"submit": {
			events: []*armadaevents.EventSequence_Event{f.Submit},
			expected: []DbOperation{InsertJobs{f.JobIdString: &schedulerdb.Job{
				JobID:          f.JobIdString,
				JobSet:         f.JobSetName,
				UserID:         f.UserId,
				Groups:         compress.MustCompressStringArray(f.Groups, compressor),
				Queue:          f.Queue,
				Queued:         true,
				QueuedVersion:  0,
				Priority:       int64(f.Priority),
				Submitted:      f.BaseTime.UnixNano(),
				SubmitMessage:  protoutil.MustMarshallAndCompress(f.Submit.GetSubmitJob(), compressor),
				SchedulingInfo: protoutil.MustMarshall(getExpectedSubmitMessageSchedulingInfo(t)),
			}}},
		},
		"ignores duplicate submit": {
			events:   []*armadaevents.EventSequence_Event{f.SubmitDuplicate},
			expected: []DbOperation{},
		},
		"job run leased": {
			events: []*armadaevents.EventSequence_Event{f.Leased},
			expected: []DbOperation{
				InsertRuns{f.RunIdUuid: &JobRunDetails{queue: f.Queue, dbRun: &schedulerdb.Run{
					RunID:    f.RunIdUuid,
					JobID:    f.JobIdString,
					JobSet:   f.JobSetName,
					Executor: f.ExecutorId,
					Node:     f.NodeName,
				}}},
				UpdateJobQueuedState{f.JobIdString: &JobQueuedStateUpdate{
					Queued:             false,
					QueuedStateVersion: 1,
				}},
			},
		},
		"job run running": {
			events:   []*armadaevents.EventSequence_Event{f.Running},
			expected: []DbOperation{MarkRunsRunning{f.RunIdUuid: true}},
		},
		"job run succeeded": {
			events:   []*armadaevents.EventSequence_Event{f.JobRunSucceeded},
			expected: []DbOperation{MarkRunsSucceeded{f.RunIdUuid: true}},
		},
		"lease returned": {
			events: []*armadaevents.EventSequence_Event{f.LeaseReturned},
			expected: []DbOperation{
				InsertJobRunErrors{f.RunIdUuid: &schedulerdb.JobRunError{
					RunID: f.RunIdUuid,
					JobID: f.JobIdString,
					Error: protoutil.MustMarshallAndCompress(f.LeaseReturned.GetJobRunErrors().Errors[0], compressor),
				}},
				MarkRunsFailed{f.RunIdUuid: &JobRunFailed{LeaseReturned: true, RunAttempted: true}},
			},
		},
		"job failed": {
			events: []*armadaevents.EventSequence_Event{f.JobRunFailed},
			expected: []DbOperation{
				InsertJobRunErrors{f.RunIdUuid: &schedulerdb.JobRunError{
					RunID: f.RunIdUuid,
					JobID: f.JobIdString,
					Error: protoutil.MustMarshallAndCompress(f.JobRunFailed.GetJobRunErrors().Errors[0], compressor),
				}},
				MarkRunsFailed{f.RunIdUuid: &JobRunFailed{LeaseReturned: false, RunAttempted: true}},
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
				UpdateJobSetPriorities{JobSetKey{queue: f.Queue, jobSet: f.JobSetName}: f.NewPriority},
			},
		},
		"JobCancelRequested": {
			events: []*armadaevents.EventSequence_Event{f.JobCancelRequested},
			expected: []DbOperation{
				MarkJobsCancelRequested{f.JobIdString: true},
			},
		},
		"JobSetCancelRequested": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequested},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobSetName}: &JobSetCancelAction{cancelQueued: true, cancelLeased: true}},
			},
		},
		"JobSetCancelRequested - Queued only": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequestedWithStateFilter(armadaevents.JobState_QUEUED)},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobSetName}: &JobSetCancelAction{cancelQueued: true, cancelLeased: false}},
			},
		},
		"JobSetCancelRequested - Pending only": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequestedWithStateFilter(armadaevents.JobState_PENDING)},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobSetName}: &JobSetCancelAction{cancelQueued: false, cancelLeased: true}},
			},
		},
		"JobSetCancelRequested - Running only": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequestedWithStateFilter(armadaevents.JobState_RUNNING)},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobSetName}: &JobSetCancelAction{cancelQueued: false, cancelLeased: true}},
			},
		},
		"JobCancelled": {
			events: []*armadaevents.EventSequence_Event{f.JobCancelled},
			expected: []DbOperation{
				MarkJobsCancelled{f.JobIdString: true},
			},
		},
		"JobRequeued": {
			events: []*armadaevents.EventSequence_Event{f.JobRequeued},
			expected: []DbOperation{
				UpdateJobQueuedState{f.JobIdString: &JobQueuedStateUpdate{
					Queued:             true,
					QueuedStateVersion: f.JobRequeued.GetJobRequeued().UpdateSequenceNumber,
				}},
				UpdateJobSchedulingInfo{f.JobIdString: &JobSchedulingInfoUpdate{
					JobSchedulingInfo:        protoutil.MustMarshall(f.JobRequeued.GetJobRequeued().SchedulingInfo),
					JobSchedulingInfoVersion: int32(f.JobRequeued.GetJobRequeued().SchedulingInfo.Version),
				}},
			},
		},
		"PositionMarker": {
			events: []*armadaevents.EventSequence_Event{f.PartitionMarker},
			expected: []DbOperation{
				&InsertPartitionMarker{markers: []*schedulerdb.Marker{
					{
						GroupID:     f.PartitionMarkerGroupIdUuid,
						PartitionID: f.PartitionMarkerPartitionId,
						Created:     f.BaseTime,
					},
				}},
			},
		},
		"multiple events": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequested, f.Running, f.JobSucceeded},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobSetName}: &JobSetCancelAction{cancelQueued: true, cancelLeased: true}},
				MarkRunsRunning{f.RunIdUuid: true},
				MarkJobsSucceeded{f.JobIdString: true},
			},
		},
		"ignored events": {
			events: []*armadaevents.EventSequence_Event{f.Running, f.JobPreempted, f.JobSucceeded},
			expected: []DbOperation{
				MarkRunsRunning{f.RunIdUuid: true},
				MarkJobsSucceeded{f.JobIdString: true},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			converter := InstructionConverter{m, f.PriorityClasses, compressor}
			es := f.NewEventSequence(tc.events...)
			results := converter.dbOperationsFromEventSequence(es)
			assertOperationsEqual(t, tc.expected, results)
		})
	}
}

func assertOperationsEqual(t *testing.T, expectedOps []DbOperation, actualOps []DbOperation) {
	t.Helper()
	require.Equal(t, len(expectedOps), len(actualOps), "operations arrays are not the same length")
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
		case InsertJobRunErrors:
			actualErrors := actualOp.(InsertJobRunErrors)
			for k, expectedError := range expectedOp.(InsertJobRunErrors) {
				actualError, ok := actualErrors[k]
				assert.True(t, ok)
				assertErrorMessagesEqual(t, expectedError.Error, actualError.Error)
				// nil out the byte arrays
				actualError.Error = nil
				expectedError.Error = nil
				assert.Equal(t, expectedError, actualError)
			}
		default:
			assert.Equal(t, expectedOp, actualOp)
		}
	}
}

func assertSchedulingInfoEqual(t *testing.T, expectedBytes []byte, actualBytes []byte) {
	actualSchedInfo, err := protoutil.Unmarshall(actualBytes, &schedulerobjects.JobSchedulingInfo{})
	assert.NoError(t, err)
	expectedSchedInfo, err := protoutil.Unmarshall(expectedBytes, &schedulerobjects.JobSchedulingInfo{})
	assert.NoError(t, err)
	assert.Equal(t, expectedSchedInfo, actualSchedInfo)
}

func assertSubmitMessagesEqual(t *testing.T, expectedBytes []byte, actualBytes []byte) {
	actualSubmitMessage, err := protoutil.DecompressAndUnmarshall(actualBytes, &armadaevents.SubmitJob{}, decompressor)
	assert.NoError(t, err)
	expectedSubmitMessage, err := protoutil.DecompressAndUnmarshall(expectedBytes, &armadaevents.SubmitJob{}, decompressor)
	assert.NoError(t, err)
	assert.Equal(t, expectedSubmitMessage, actualSubmitMessage)
}

func assertErrorMessagesEqual(t *testing.T, expectedBytes []byte, actualBytes []byte) {
	actualError, err := protoutil.DecompressAndUnmarshall(actualBytes, &armadaevents.Error{}, decompressor)
	assert.NoError(t, err)
	expectedError, err := protoutil.DecompressAndUnmarshall(expectedBytes, &armadaevents.Error{}, decompressor)
	assert.NoError(t, err)
	assert.Equal(t, expectedError, actualError)
}

func getExpectedSubmitMessageSchedulingInfo(t *testing.T) *schedulerobjects.JobSchedulingInfo {
	expectedSubmitSchedulingInfo := &schedulerobjects.JobSchedulingInfo{
		Lifetime:        0,
		AtMostOnce:      true,
		Preemptible:     true,
		ConcurrencySafe: true,
		Version:         0,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector:     f.NodeSelector,
						Tolerations:      f.Tolerations,
						PreemptionPolicy: "PreemptLowerPriority",
						Priority:         f.PriorityClassValue,
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
	}
	return expectedSubmitSchedulingInfo
}

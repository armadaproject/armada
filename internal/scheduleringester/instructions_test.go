package scheduleringester

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	f "github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

var (
	compressor   = compress.NewThreadSafeZlibCompressor(1024)
	decompressor = compress.NewThreadSafeZlibDecompressor()
	m            = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "test_")
)

func TestConvertEventSequence(t *testing.T) {
	tests := map[string]struct {
		events   []*armadaevents.EventSequence_Event
		expected []DbOperation
	}{
		"submit": {
			events: []*armadaevents.EventSequence_Event{f.Submit},
			expected: []DbOperation{InsertJobs{f.JobId: &schedulerdb.Job{
				JobID:          f.JobId,
				JobSet:         f.JobsetName,
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
		"job run leased": {
			events: []*armadaevents.EventSequence_Event{f.Leased},
			expected: []DbOperation{
				InsertRuns{f.RunId: &JobRunDetails{Queue: f.Queue, DbRun: &schedulerdb.Run{
					RunID:                  f.RunId,
					JobID:                  f.JobId,
					JobSet:                 f.JobsetName,
					Queue:                  f.Queue,
					Executor:               f.ExecutorId,
					Node:                   f.NodeName,
					Pool:                   f.Pool,
					ScheduledAtPriority:    &f.ScheduledAtPriority,
					Created:                f.BaseTime.UnixNano(),
					LeasedTimestamp:        &f.BaseTime,
					PodRequirementsOverlay: protoutil.MustMarshall(f.Leased.GetJobRunLeased().GetPodRequirementsOverlay()),
				}}},
				UpdateJobQueuedState{f.JobId: &JobQueuedStateUpdate{
					Queued:             false,
					QueuedStateVersion: 1,
				}},
			},
		},
		"job run running": {
			events:   []*armadaevents.EventSequence_Event{f.Running},
			expected: []DbOperation{MarkRunsRunning{f.RunId: f.BaseTime}},
		},
		"job run succeeded": {
			events:   []*armadaevents.EventSequence_Event{f.JobRunSucceeded},
			expected: []DbOperation{MarkRunsSucceeded{f.RunId: f.BaseTime}},
		},
		"job run pending": {
			events:   []*armadaevents.EventSequence_Event{f.Assigned},
			expected: []DbOperation{MarkRunsPending{f.RunId: f.BaseTime}},
		},
		"job preemption requested": {
			events:   []*armadaevents.EventSequence_Event{f.JobPreemptionRequested},
			expected: []DbOperation{MarkRunsForJobPreemptRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: []string{f.JobId}}},
		},
		"job run preempted": {
			events:   []*armadaevents.EventSequence_Event{f.JobRunPreempted},
			expected: []DbOperation{MarkRunsPreempted{f.RunId: f.BaseTime}},
		},
		"lease returned": {
			events: []*armadaevents.EventSequence_Event{f.LeaseReturned},
			expected: []DbOperation{
				InsertJobRunErrors{f.RunId: &schedulerdb.JobRunError{
					RunID: f.RunId,
					JobID: f.JobId,
					Error: protoutil.MustMarshallAndCompress(f.LeaseReturned.GetJobRunErrors().Errors[0], compressor),
				}},
				MarkRunsFailed{f.RunId: &JobRunFailed{LeaseReturned: true, RunAttempted: true, FailureTime: f.BaseTime}},
			},
		},
		"job failed": {
			events: []*armadaevents.EventSequence_Event{f.JobRunFailed},
			expected: []DbOperation{
				InsertJobRunErrors{f.RunId: &schedulerdb.JobRunError{
					RunID: f.RunId,
					JobID: f.JobId,
					Error: protoutil.MustMarshallAndCompress(f.JobRunFailed.GetJobRunErrors().Errors[0], compressor),
				}},
				MarkRunsFailed{f.RunId: &JobRunFailed{LeaseReturned: false, RunAttempted: true, FailureTime: f.BaseTime}},
			},
		},
		"job errors terminal": {
			events: []*armadaevents.EventSequence_Event{f.JobFailed},
			expected: []DbOperation{
				MarkJobsFailed{f.JobId: true},
			},
		},
		"job succeeded": {
			events: []*armadaevents.EventSequence_Event{f.JobSucceeded},
			expected: []DbOperation{
				MarkJobsSucceeded{f.JobId: true},
			},
		},
		"reprioritise job": {
			events: []*armadaevents.EventSequence_Event{f.JobReprioritiseRequested},
			expected: []DbOperation{
				&UpdateJobPriorities{
					key: JobReprioritiseKey{
						JobSetKey: JobSetKey{queue: f.Queue, jobSet: f.JobsetName},
						Priority:  f.NewPriority,
					},
					jobIds: []string{f.JobId},
				},
			},
		},
		"reprioritise jobset": {
			events: []*armadaevents.EventSequence_Event{f.JobSetReprioritiseRequested},
			expected: []DbOperation{
				UpdateJobSetPriorities{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: f.NewPriority},
			},
		},
		"JobCancelRequested": {
			events: []*armadaevents.EventSequence_Event{f.JobCancelRequested},
			expected: []DbOperation{
				MarkJobsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: {f.JobId}},
			},
		},
		"JobSetCancelRequested": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequested},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: &JobSetCancelAction{cancelQueued: true, cancelLeased: true}},
			},
		},
		"JobSetCancelRequested - Queued only": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequestedWithStateFilter(armadaevents.JobState_QUEUED)},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: &JobSetCancelAction{cancelQueued: true, cancelLeased: false}},
			},
		},
		"JobSetCancelRequested - Pending only": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequestedWithStateFilter(armadaevents.JobState_PENDING)},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: &JobSetCancelAction{cancelQueued: false, cancelLeased: true}},
			},
		},
		"JobSetCancelRequested - Running only": {
			events: []*armadaevents.EventSequence_Event{f.JobSetCancelRequestedWithStateFilter(armadaevents.JobState_RUNNING)},
			expected: []DbOperation{
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: &JobSetCancelAction{cancelQueued: false, cancelLeased: true}},
			},
		},
		"JobCancelled": {
			events: []*armadaevents.EventSequence_Event{f.JobCancelled},
			expected: []DbOperation{
				MarkJobsCancelled{f.JobId: f.BaseTime},
			},
		},
		"JobRequeued": {
			events: []*armadaevents.EventSequence_Event{f.JobRequeued},
			expected: []DbOperation{
				UpdateJobQueuedState{f.JobId: &JobQueuedStateUpdate{
					Queued:             true,
					QueuedStateVersion: f.JobRequeued.GetJobRequeued().UpdateSequenceNumber,
				}},
				UpdateJobSchedulingInfo{f.JobId: &JobSchedulingInfoUpdate{
					JobSchedulingInfo:        protoutil.MustMarshall(f.JobRequeued.GetJobRequeued().SchedulingInfo),
					JobSchedulingInfoVersion: int32(f.JobRequeued.GetJobRequeued().SchedulingInfo.Version),
				}},
			},
		},
		"SubmitChecked": {
			events: []*armadaevents.EventSequence_Event{f.JobValidated},
			expected: []DbOperation{
				MarkJobsValidated{f.JobId: []string{"cpu"}},
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
				MarkJobSetsCancelRequested{JobSetKey{queue: f.Queue, jobSet: f.JobsetName}: &JobSetCancelAction{cancelQueued: true, cancelLeased: true}},
				MarkRunsRunning{f.RunId: f.BaseTime},
				MarkJobsSucceeded{f.JobId: true},
			},
		},
		"multiple events - multiple timestamps": {
			events: multipleEventsMultipleTimeStamps(),
			expected: []DbOperation{
				MarkJobsCancelled{f.JobId: f.BaseTime},
				MarkRunsSucceeded{f.RunId: f.BaseTime},
				MarkRunsRunning{f.RunId: f.BaseTime},
				MarkJobsCancelled{f.JobId: f.BaseTime.Add(time.Hour)},
				MarkRunsSucceeded{f.RunId: f.BaseTime.Add(time.Hour)},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			converter := JobSetEventsInstructionConverter{m, compressor}
			es := f.NewEventSequence(tc.events...)
			results := converter.dbOperationsFromEventSequence(es)
			assertOperationsEqual(t, tc.expected, results)
		})
	}
}

func TestConvertControlPlaneEvent(t *testing.T) {
	tests := map[string]struct {
		event    *controlplaneevents.Event
		expected []DbOperation
	}{
		"upsert cordon executor event": {
			event: f.UpsertExecutorSettingsCordon,
			expected: []DbOperation{UpsertExecutorSettings{
				f.ExecutorId: &ExecutorSettingsUpsert{
					ExecutorID:   f.ExecutorId,
					Cordoned:     true,
					CordonReason: f.ExecutorCordonReason,
				},
			}},
		},
		"upsert uncordon executor event": {
			event: f.UpsertExecutorSettingsUncordon,
			expected: []DbOperation{UpsertExecutorSettings{
				f.ExecutorId: &ExecutorSettingsUpsert{
					ExecutorID:   f.ExecutorId,
					Cordoned:     false,
					CordonReason: "",
				},
			}},
		},
		"delete executor settings": {
			event: f.DeleteExecutorSettings,
			expected: []DbOperation{DeleteExecutorSettings{
				f.ExecutorId: &ExecutorSettingsDelete{
					ExecutorID: f.ExecutorId,
				},
			}},
		},
		"preempt on executor": {
			event: f.PreemptOnExecutor,
			expected: []DbOperation{PreemptExecutor{
				f.ExecutorId: &PreemptOnExecutor{
					Name:            f.ExecutorId,
					Queues:          []string{f.Queue},
					PriorityClasses: []string{f.PriorityClassName},
				},
			}},
		},
		"cancel on executor": {
			event: f.CancelOnExecutor,
			expected: []DbOperation{CancelExecutor{
				f.ExecutorId: &CancelOnExecutor{
					Name:            f.ExecutorId,
					Queues:          []string{f.Queue},
					PriorityClasses: []string{f.PriorityClassName},
				},
			}},
		},
		"preempt on queue": {
			event: f.PreemptOnQueue,
			expected: []DbOperation{PreemptQueue{
				f.Queue: &PreemptOnQueue{
					Name:            f.Queue,
					PriorityClasses: []string{f.PriorityClassName},
				},
			}},
		},
		"cancel queued on queue": {
			event: f.CancelQueuedOnQueue,
			expected: []DbOperation{CancelQueue{
				f.Queue: &CancelOnQueue{
					Name:            f.Queue,
					PriorityClasses: []string{f.PriorityClassName},
					JobStates:       []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_QUEUED},
				},
			}},
		},
		"cancel running on queue": {
			event: f.CancelRunningOnQueue,
			expected: []DbOperation{CancelQueue{
				f.Queue: &CancelOnQueue{
					Name:            f.Queue,
					PriorityClasses: []string{f.PriorityClassName},
					JobStates:       []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_RUNNING},
				},
			}},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			converter := ControlPlaneEventsInstructionConverter{m}
			results := converter.dbOperationFromControlPlaneEvent(tc.event)
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
		Lifetime:          0,
		AtMostOnce:        true,
		Preemptible:       true,
		ConcurrencySafe:   true,
		Version:           0,
		PriorityClassName: "test-priority",
		Priority:          3,
		SubmitTime:        protoutil.ToTimestamp(f.BaseTime),
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector: f.NodeSelector,
						Tolerations: armadaslices.Map(f.Tolerations, func(t v1.Toleration) *v1.Toleration {
							return &t
						}),
						PreemptionPolicy: "PreemptLowerPriority",
						ResourceRequirements: &v1.ResourceRequirements{
							Limits: map[v1.ResourceName]resource.Quantity{
								"memory": resource.MustParse("64Mi"),
								"cpu":    resource.MustParse("150m"),
							},
							Requests: map[v1.ResourceName]resource.Quantity{
								"memory": resource.MustParse("64Mi"),
								"cpu":    resource.MustParse("150m"),
							},
						},
						Annotations: map[string]string{
							configuration.FailFastAnnotation: "true",
						},
					},
				},
			},
		},
	}
	return expectedSubmitSchedulingInfo
}

func multipleEventsMultipleTimeStamps() []*armadaevents.EventSequence_Event {
	events := []*armadaevents.EventSequence_Event{f.JobCancelled, f.JobRunSucceeded, f.Running}
	created := protoutil.ToTimestamp(f.BaseTime.Add(time.Hour))
	anotherCancelled, _ := f.DeepCopy(f.JobCancelled)
	anotherSucceeded, _ := f.DeepCopy(f.JobRunSucceeded)
	anotherCancelled.Created = created
	anotherSucceeded.Created = created
	return append(events, anotherCancelled, anotherSucceeded)
}

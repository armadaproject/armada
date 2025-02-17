package instructions

import (
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	cpu                  = 12500
	memory               = 2000 * 1024 * 1024 * 1024
	ephemeralStorage     = 3000 * 1024 * 1024 * 1024
	gpu                  = 8
	priorityClass        = "default"
	userAnnotationPrefix = "test_prefix/"
)

var expectedLeased = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobLeasedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
	LatestRunId:               pointer.String(testfixtures.RunId),
}

var expectedPending = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobPendingOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
	LatestRunId:               pointer.String(testfixtures.RunId),
}

var expectedRunning = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobRunningOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
	LatestRunId:               pointer.String(testfixtures.RunId),
}

var expectedLeasedRun = model.CreateJobRunInstruction{
	RunId:       testfixtures.RunId,
	JobId:       testfixtures.JobId,
	Cluster:     testfixtures.ExecutorId,
	Leased:      &testfixtures.BaseTime,
	Node:        pointer.String(testfixtures.NodeName),
	JobRunState: lookout.JobRunLeasedOrdinal,
}

var expectedPendingRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunId,
	Pending:     &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunPendingOrdinal),
}

var expectedRunningRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunId,
	Node:        pointer.String(testfixtures.NodeName),
	Started:     &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunRunningOrdinal),
}

var expectedJobRunSucceeded = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunId,
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal),
	ExitCode:    pointer.Int32(0),
}

var expectedJobSucceeded = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobSucceededOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedJobRequeued = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobQueuedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedJobCancelled = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobCancelledOrdinal),
	Cancelled:                 &testfixtures.BaseTime,
	CancelUser:                pointer.String(testfixtures.CancelUser),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedJobReprioritised = model.UpdateJobInstruction{
	JobId:    testfixtures.JobId,
	Priority: pointer.Int64(testfixtures.NewPriority),
}

var expectedFailed = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobFailedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedFailedRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunId,
	Node:        pointer.String(testfixtures.NodeName),
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunFailedOrdinal),
	Error:       []byte(testfixtures.ErrMsg),
	Debug:       []byte(testfixtures.DebugMsg),
	ExitCode:    pointer.Int32(testfixtures.ExitCode),
}

var expectedUnschedulable = model.UpdateJobRunInstruction{
	RunId: testfixtures.RunId,
	Node:  pointer.String(testfixtures.NodeName),
}

var expectedRejected = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobRejectedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedRejectedJobError = model.CreateJobErrorInstruction{
	JobId: testfixtures.JobId,
	Error: []byte(testfixtures.ErrMsg),
}

var expectedPreempted = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobId,
	State:                     pointer.Int32(lookout.JobPreemptedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedPreemptedRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunId,
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunPreemptedOrdinal),
	Error:       []byte(testfixtures.PreemptionReason),
}

var expectedCancelledRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunId,
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunCancelledOrdinal),
}

func TestConvert(t *testing.T) {
	submit, err := testfixtures.DeepCopy(testfixtures.Submit)
	assert.NoError(t, err)
	resources := map[v1.ResourceName]resource.Quantity{
		"cpu":               resource.MustParse("12500m"),
		"memory":            resource.MustParse("2000Gi"),
		"ephemeral-storage": resource.MustParse("3000Gi"),
		"nvidia.com/gpu":    resource.MustParse("8"),
	}
	submit.GetSubmitJob().GetMainObject().GetPodSpec().GetPodSpec().Containers[0].Resources = v1.ResourceRequirements{
		Limits:   resources,
		Requests: resources,
	}
	submit.GetSubmitJob().GetMainObject().GetPodSpec().GetPodSpec().PriorityClassName = priorityClass
	submit.GetSubmitJob().GetObjectMeta().Annotations = map[string]string{
		userAnnotationPrefix + "a":        "0",
		"b":                               "1",
		"armadaproject.io/externalJobUri": "external-job-uri",
	}
	job, err := eventutil.ApiJobFromLogSubmitJob(testfixtures.UserId, []string{}, testfixtures.Queue, testfixtures.JobsetName, testfixtures.BaseTime, submit.GetSubmitJob())
	assert.NoError(t, err)
	jobProto, err := proto.Marshal(job)
	assert.NoError(t, err)
	expectedSubmit := &model.CreateJobInstruction{
		JobId:                     testfixtures.JobId,
		Queue:                     testfixtures.Queue,
		Owner:                     testfixtures.UserId,
		Namespace:                 testfixtures.Namespace,
		JobSet:                    testfixtures.JobsetName,
		Cpu:                       cpu,
		Memory:                    memory,
		EphemeralStorage:          ephemeralStorage,
		Gpu:                       gpu,
		Priority:                  testfixtures.Priority,
		Submitted:                 testfixtures.BaseTime,
		State:                     lookout.JobQueuedOrdinal,
		LastTransitionTime:        testfixtures.BaseTime,
		LastTransitionTimeSeconds: testfixtures.BaseTime.Unix(),
		JobProto:                  jobProto,
		PriorityClass:             pointer.String(priorityClass),
		Annotations: map[string]string{
			"a":                               "0",
			"b":                               "1",
			"armadaproject.io/externalJobUri": "external-job-uri",
		},
		ExternalJobUri: "external-job-uri",
	}

	cancelledWithReason, err := testfixtures.DeepCopy(testfixtures.JobCancelled)
	assert.NoError(t, err)
	cancelledWithReason.GetCancelledJob().Reason = "some reason"

	tests := map[string]struct {
		events   *utils.EventsWithIds[*armadaevents.EventSequence]
		expected *model.InstructionSet
	}{
		"submit": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events: []*armadaevents.EventSequence{testfixtures.NewEventSequence(submit)},
				MessageIds: []pulsar.MessageID{
					pulsarutils.NewMessageId(1),
				},
			},
			expected: &model.InstructionSet{
				JobsToCreate: []*model.CreateJobInstruction{expectedSubmit},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"happy path single update": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events: []*armadaevents.EventSequence{testfixtures.NewEventSequence(
					submit,
					testfixtures.Leased,
					testfixtures.Assigned,
					testfixtures.Running,
					testfixtures.JobRunSucceeded,
					testfixtures.JobSucceeded,
				)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToCreate:    []*model.CreateJobInstruction{expectedSubmit},
				JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedPending, &expectedRunning, &expectedJobSucceeded},
				JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedPendingRun, &expectedRunningRun, &expectedJobRunSucceeded},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"happy path multi update": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events: []*armadaevents.EventSequence{
					testfixtures.NewEventSequence(submit),
					testfixtures.NewEventSequence(testfixtures.Leased),
					testfixtures.NewEventSequence(testfixtures.Assigned),
					testfixtures.NewEventSequence(testfixtures.Running),
					testfixtures.NewEventSequence(testfixtures.JobRunSucceeded),
					testfixtures.NewEventSequence(testfixtures.JobSucceeded),
				},
				MessageIds: []pulsar.MessageID{
					pulsarutils.NewMessageId(1),
					pulsarutils.NewMessageId(2),
					pulsarutils.NewMessageId(3),
					pulsarutils.NewMessageId(4),
					pulsarutils.NewMessageId(5),
				},
			},
			expected: &model.InstructionSet{
				JobsToCreate:    []*model.CreateJobInstruction{expectedSubmit},
				JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedPending, &expectedRunning, &expectedJobSucceeded},
				JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedPendingRun, &expectedRunningRun, &expectedJobRunSucceeded},
				MessageIds: []pulsar.MessageID{
					pulsarutils.NewMessageId(1),
					pulsarutils.NewMessageId(2),
					pulsarutils.NewMessageId(3),
					pulsarutils.NewMessageId(4),
					pulsarutils.NewMessageId(5),
				},
			},
		},
		"requeued": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRequeued)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobRequeued},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job cancelled": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobCancelled)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobCancelled},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job cancelled with reason": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(cancelledWithReason)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{{
					JobId:                     testfixtures.JobId,
					State:                     pointer.Int32(lookout.JobCancelledOrdinal),
					CancelReason:              pointer.String("some reason"),
					CancelUser:                pointer.String(testfixtures.CancelUser),
					Cancelled:                 &testfixtures.BaseTime,
					LastTransitionTime:        &testfixtures.BaseTime,
					LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
				}},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job run cancelled": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunCancelled)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedCancelledRun},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"reprioritized": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobReprioritised)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobReprioritised},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job run failed": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunFailed)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedFailedRun},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job failed": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobFailed)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedFailed},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"unschedulable": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunUnschedulable)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedUnschedulable},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job rejected": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRejected)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate:      []*model.UpdateJobInstruction{&expectedRejected},
				JobErrorsToCreate: []*model.CreateJobErrorInstruction{&expectedRejectedJobError},
				MessageIds:        []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job preempted": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobPreempted)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedPreempted},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job run preempted": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events:     []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunPreempted)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedPreemptedRun},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"invalid event without created time": {
			events: &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events: []*armadaevents.EventSequence{
					testfixtures.NewEventSequence(&armadaevents.EventSequence_Event{
						Event: &armadaevents.EventSequence_Event_JobRunRunning{
							JobRunRunning: &armadaevents.JobRunRunning{
								RunId: testfixtures.RunId,
								JobId: testfixtures.JobId,
								ResourceInfos: []*armadaevents.KubernetesResourceInfo{
									{
										Info: &armadaevents.KubernetesResourceInfo_PodInfo{
											PodInfo: &armadaevents.PodInfo{
												NodeName:  testfixtures.NodeName,
												PodNumber: testfixtures.PodNumber,
											},
										},
									},
								},
							},
						},
					}),
					testfixtures.NewEventSequence(submit),
				},
				MessageIds: []pulsar.MessageID{
					pulsarutils.NewMessageId(1),
					pulsarutils.NewMessageId(2),
				},
			},
			expected: &model.InstructionSet{
				JobsToCreate: []*model.CreateJobInstruction{expectedSubmit},
				MessageIds: []pulsar.MessageID{
					pulsarutils.NewMessageId(1),
					pulsarutils.NewMessageId(2),
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			converter := NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, &compress.NoOpCompressor{})
			decompressor := &compress.NoOpDecompressor{}
			instructionSet := converter.Convert(armadacontext.TODO(), tc.events)
			require.Equal(t, len(tc.expected.JobsToCreate), len(instructionSet.JobsToCreate))
			// The value of JobProto is not deterministic, because annotations
			// are stored in a map[string]string; compare the deserialized
			// versions of this field instead, and zero it out before calling
			// assert.Equal.
			for i, expected := range tc.expected.JobsToCreate {
				expected := *expected
				actual := *instructionSet.JobsToCreate[i]

				var expectedApiJob api.Job
				var actualApiJob api.Job
				assert.Equal(
					t,
					protoutil.MustDecompressAndUnmarshall(expected.JobProto, &expectedApiJob, decompressor),
					protoutil.MustDecompressAndUnmarshall(actual.JobProto, &actualApiJob, decompressor),
				)

				expected.JobProto = nil
				actual.JobProto = nil
				assert.Equal(t, expected, actual)
			}
			assert.Equal(t, tc.expected.JobsToUpdate, instructionSet.JobsToUpdate)
			assert.Equal(t, tc.expected.JobRunsToCreate, instructionSet.JobRunsToCreate)
			assert.Equal(t, tc.expected.JobRunsToUpdate, instructionSet.JobRunsToUpdate)
			assert.Equal(t, tc.expected.MessageIds, instructionSet.MessageIds)
		})
	}
}

func TestTruncatesStringsThatAreTooLong(t *testing.T) {
	longString := strings.Repeat("x", 4000)

	submit, err := testfixtures.DeepCopy(testfixtures.Submit)
	assert.NoError(t, err)
	submit.GetSubmitJob().GetMainObject().GetPodSpec().GetPodSpec().PriorityClassName = longString
	submit.GetSubmitJob().GetObjectMeta().Annotations = map[string]string{
		"armadaproject.io/externalJobUri": longString,
	}

	leased, err := testfixtures.DeepCopy(testfixtures.Leased)
	assert.NoError(t, err)
	leased.GetJobRunLeased().ExecutorId = longString
	leased.GetJobRunLeased().NodeId = longString

	assigned, err := testfixtures.DeepCopy(testfixtures.Assigned)
	assert.NoError(t, err)

	running, err := testfixtures.DeepCopy(testfixtures.Running)
	assert.NoError(t, err)
	running.GetJobRunRunning().GetResourceInfos()[0].GetPodInfo().NodeName = longString

	events := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: []*armadaevents.EventSequence{
			{
				Queue:      longString,
				JobSetName: longString,
				UserId:     longString,
				Events: []*armadaevents.EventSequence_Event{
					submit,
					leased,
					assigned,
					running,
				},
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}

	converter := NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, &compress.NoOpCompressor{})
	actual := converter.Convert(armadacontext.TODO(), events)

	// String lengths obtained from database schema
	assert.Len(t, actual.JobsToCreate[0].Queue, 512)
	assert.Len(t, actual.JobsToCreate[0].Owner, 512)
	assert.Len(t, actual.JobsToCreate[0].JobSet, 1024)
	assert.Len(t, *actual.JobsToCreate[0].PriorityClass, 63)
	assert.Len(t, actual.JobsToCreate[0].ExternalJobUri, 1024)
	assert.Len(t, actual.JobRunsToCreate[0].Cluster, 512)
	assert.Len(t, *actual.JobRunsToCreate[0].Node, 512)
	assert.Len(t, *actual.JobRunsToUpdate[1].Node, 512)
}

func TestExtractNodeName(t *testing.T) {
	podError := armadaevents.PodError{}
	assert.Nil(t, extractNodeName(&podError))
	podError.NodeName = testfixtures.NodeName
	assert.Equal(t, pointer.String(testfixtures.NodeName), extractNodeName(&podError))
}

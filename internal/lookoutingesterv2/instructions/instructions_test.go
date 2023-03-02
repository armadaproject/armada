package instructions

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
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
	JobId:                     testfixtures.JobIdString,
	State:                     pointer.Int32(lookout.JobPendingOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
	LatestRunId:               pointer.String(testfixtures.RunIdString),
}

var expectedRunning = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobIdString,
	State:                     pointer.Int32(lookout.JobRunningOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
	LatestRunId:               pointer.String(testfixtures.RunIdString),
}

var expectedLeasedRun = model.CreateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	JobId:       testfixtures.JobIdString,
	Cluster:     testfixtures.ExecutorId,
	Pending:     testfixtures.BaseTime,
	JobRunState: lookout.JobRunPendingOrdinal,
}

var expectedRunningRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	Node:        pointer.String(testfixtures.NodeName),
	Started:     &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunRunningOrdinal),
}

var expectedJobRunSucceeded = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal),
	ExitCode:    pointer.Int32(0),
}

var expectedJobSucceeded = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobIdString,
	State:                     pointer.Int32(lookout.JobSucceededOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedJobCancelled = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobIdString,
	State:                     pointer.Int32(lookout.JobCancelledOrdinal),
	Cancelled:                 &testfixtures.BaseTime,
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedJobReprioritised = model.UpdateJobInstruction{
	JobId:    testfixtures.JobIdString,
	Priority: pointer.Int64(testfixtures.NewPriority),
}

var expectedFailed = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobIdString,
	State:                     pointer.Int32(lookout.JobFailedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
}

var expectedFailedRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	Node:        pointer.String(testfixtures.NodeName),
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunFailedOrdinal),
	Error:       []byte(testfixtures.ErrMsg),
	ExitCode:    pointer.Int32(testfixtures.ExitCode),
}

var expectedTerminated = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	Node:        pointer.String(testfixtures.NodeName),
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunTerminatedOrdinal),
	Error:       []byte(testfixtures.TerminatedMsg),
}

var expectedUnschedulable = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	Node:        pointer.String(testfixtures.NodeName),
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunUnableToScheduleOrdinal),
	Error:       []byte(testfixtures.UnschedulableMsg),
}

var expectedPreempted = model.UpdateJobInstruction{
	JobId:                     testfixtures.JobIdString,
	State:                     pointer.Int32(lookout.JobPreemptedOrdinal),
	LastTransitionTime:        &testfixtures.BaseTime,
	LastTransitionTimeSeconds: pointer.Int64(testfixtures.BaseTime.Unix()),
	LatestRunId:               pointer.String(testfixtures.RunIdString),
}

var expectedPreemptedRun = model.UpdateJobRunInstruction{
	RunId:       testfixtures.RunIdString,
	Finished:    &testfixtures.BaseTime,
	JobRunState: pointer.Int32(lookout.JobRunPreemptedOrdinal),
	Error:       []byte("preempted by non armada pod"),
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
	job, err := eventutil.ApiJobFromLogSubmitJob(testfixtures.UserId, []string{}, testfixtures.Queue, testfixtures.JobSetName, testfixtures.BaseTime, submit.GetSubmitJob())
	assert.NoError(t, err)
	jobProto, err := proto.Marshal(job)
	assert.NoError(t, err)
	expectedSubmit := &model.CreateJobInstruction{
		JobId:                     testfixtures.JobIdString,
		Queue:                     testfixtures.Queue,
		Owner:                     testfixtures.UserId,
		JobSet:                    testfixtures.JobSetName,
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
	}

	otherJobIdUlid := util.ULID()
	otherJobId := util.StringFromUlid(otherJobIdUlid)
	otherJobIdProto := armadaevents.ProtoUuidFromUlid(otherJobIdUlid)

	otherRunIdUuid, err := uuid.NewUUID()
	assert.NoError(t, err)
	otherRunIdProto := armadaevents.ProtoUuidFromUuid(otherRunIdUuid)

	preempted, err := testfixtures.DeepCopy(testfixtures.JobPreempted)
	assert.NoError(t, err)
	preempted.GetJobRunPreempted().PreemptiveJobId = otherJobIdProto
	preempted.GetJobRunPreempted().PreemptiveRunId = otherRunIdProto

	preemptedWithPrempteeWithZeroId, err := testfixtures.DeepCopy(testfixtures.JobPreempted)
	assert.NoError(t, err)
	preemptedWithPrempteeWithZeroId.GetJobRunPreempted().PreemptiveJobId = &armadaevents.Uuid{}
	preemptedWithPrempteeWithZeroId.GetJobRunPreempted().PreemptiveRunId = &armadaevents.Uuid{}

	tests := map[string]struct {
		events   *ingest.EventSequencesWithIds
		expected *model.InstructionSet
	}{
		"submit": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(submit)},
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
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(
					submit,
					testfixtures.Assigned,
					testfixtures.Running,
					testfixtures.JobRunSucceeded,
					testfixtures.JobSucceeded,
				)},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToCreate:    []*model.CreateJobInstruction{expectedSubmit},
				JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedRunning, &expectedJobSucceeded},
				JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun, &expectedJobRunSucceeded},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"happy path multi update": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{
					testfixtures.NewEventSequence(submit),
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
				JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedRunning, &expectedJobSucceeded},
				JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun, &expectedJobRunSucceeded},
				MessageIds: []pulsar.MessageID{
					pulsarutils.NewMessageId(1),
					pulsarutils.NewMessageId(2),
					pulsarutils.NewMessageId(3),
					pulsarutils.NewMessageId(4),
					pulsarutils.NewMessageId(5),
				},
			},
		},
		"cancelled": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobCancelled)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobCancelled},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"reprioritized": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobReprioritised)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobReprioritised},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job run failed": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunFailed)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedFailedRun},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"job failed": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobFailed)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedFailed},
				MessageIds:   []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"terminated": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunTerminated)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedTerminated},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"unschedulable": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobRunUnschedulable)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedUnschedulable},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"preempted": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobPreempted)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate:    []*model.UpdateJobInstruction{&expectedPreempted},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedPreemptedRun},
				MessageIds:      []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"preempted with preemptee": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(preempted)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedPreempted},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
					RunId:       testfixtures.RunIdString,
					Finished:    &testfixtures.BaseTime,
					JobRunState: pointer.Int32(lookout.JobRunPreemptedOrdinal),
					Error:       []byte(fmt.Sprintf("preempted by job %s", otherJobId)),
				}},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"preempted with zeroed preemptee id": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(preemptedWithPrempteeWithZeroId)},
				MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobsToUpdate: []*model.UpdateJobInstruction{&expectedPreempted},
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
					RunId:       testfixtures.RunIdString,
					Finished:    &testfixtures.BaseTime,
					JobRunState: pointer.Int32(lookout.JobRunPreemptedOrdinal),
					Error:       []byte("preempted by non armada pod"),
				}},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
		},
		"invalid event without job id or run id": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{
					testfixtures.NewEventSequence(&armadaevents.EventSequence_Event{
						Created: &testfixtures.BaseTime,
						Event: &armadaevents.EventSequence_Event_JobRunRunning{
							JobRunRunning: &armadaevents.JobRunRunning{},
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
		"invalid event without created time": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{
					testfixtures.NewEventSequence(&armadaevents.EventSequence_Event{
						Event: &armadaevents.EventSequence_Event_JobRunRunning{
							JobRunRunning: &armadaevents.JobRunRunning{
								RunId: testfixtures.RunIdProto,
								JobId: testfixtures.JobIdProto,
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
			converter := NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
			instructionSet := converter.Convert(context.TODO(), tc.events)
			assert.Equal(t, tc.expected, instructionSet)
		})
	}
}

func TestFailedWithMissingRunId(t *testing.T) {
	converter := NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
	instructions := converter.Convert(context.Background(), &ingest.EventSequencesWithIds{
		EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(testfixtures.JobLeaseReturned)},
		MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	})
	jobRun := instructions.JobRunsToCreate[0]
	assert.NotEqual(t, eventutil.LEGACY_RUN_ID, jobRun.RunId)
	expected := &model.InstructionSet{
		JobRunsToCreate: []*model.CreateJobRunInstruction{
			{
				JobId:       testfixtures.JobIdString,
				RunId:       jobRun.RunId,
				Cluster:     testfixtures.ExecutorId,
				Pending:     testfixtures.BaseTime,
				JobRunState: lookout.JobRunPendingOrdinal,
			},
		},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{
			{
				RunId:       jobRun.RunId,
				Started:     &testfixtures.BaseTime,
				Finished:    &testfixtures.BaseTime,
				JobRunState: pointer.Int32(lookout.JobRunLeaseReturnedOrdinal),
				Error:       []byte(testfixtures.LeaseReturnedMsg),
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
}

func TestTruncatesStringsThatAreTooLong(t *testing.T) {
	longString := strings.Repeat("x", 4000)

	submit, err := testfixtures.DeepCopy(testfixtures.Submit)
	assert.NoError(t, err)
	submit.GetSubmitJob().GetMainObject().GetPodSpec().GetPodSpec().PriorityClassName = longString

	assigned, err := testfixtures.DeepCopy(testfixtures.Assigned)
	assert.NoError(t, err)
	assigned.GetJobRunAssigned().GetResourceInfos()[0].GetObjectMeta().ExecutorId = longString

	running, err := testfixtures.DeepCopy(testfixtures.Running)
	assert.NoError(t, err)
	running.GetJobRunRunning().GetResourceInfos()[0].GetPodInfo().NodeName = longString

	events := &ingest.EventSequencesWithIds{
		EventSequences: []*armadaevents.EventSequence{{
			Queue:      longString,
			JobSetName: longString,
			UserId:     longString,
			Events: []*armadaevents.EventSequence_Event{
				submit,
				assigned,
				running,
			},
		}},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}

	converter := NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
	actual := converter.Convert(context.TODO(), events)

	// String lengths obtained from database schema
	assert.Len(t, actual.JobsToCreate[0].Queue, 512)
	assert.Len(t, actual.JobsToCreate[0].Owner, 512)
	assert.Len(t, actual.JobsToCreate[0].JobSet, 1024)
	assert.Len(t, *actual.JobsToCreate[0].PriorityClass, 63)
	assert.Len(t, actual.JobRunsToCreate[0].Cluster, 512)
	assert.Len(t, *actual.JobRunsToUpdate[0].Node, 512)
}

func TestAnnotations(t *testing.T) {
	annotations := map[string]string{userAnnotationPrefix + "a": "b", "1": "2"}
	expected := []*model.CreateUserAnnotationInstruction{
		{
			JobId:  testfixtures.JobIdString,
			Key:    "1",
			Value:  "2",
			Queue:  testfixtures.Queue,
			Jobset: testfixtures.JobSetName,
		},
		{
			JobId:  testfixtures.JobIdString,
			Key:    "a",
			Value:  "b",
			Queue:  testfixtures.Queue,
			Jobset: testfixtures.JobSetName,
		},
	}
	annotationInstructions := extractAnnotations(testfixtures.JobIdString, testfixtures.Queue, testfixtures.JobSetName, annotations, userAnnotationPrefix)
	assert.Equal(t, expected, annotationInstructions)
}

func TestExtractNodeName(t *testing.T) {
	podError := armadaevents.PodError{}
	assert.Nil(t, extractNodeName(&podError))
	podError.NodeName = testfixtures.NodeName
	assert.Equal(t, pointer.String(testfixtures.NodeName), extractNodeName(&podError))
}

package instructions

import (
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/database/lookout"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/common/ingest/testfixtures"
	"github.com/G-Research/armada/internal/lookoutingesterv2/metrics"
	"github.com/G-Research/armada/internal/lookoutingesterv2/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
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
		"job run failed with null char in error": {
			events: &ingest.EventSequencesWithIds{
				EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(&armadaevents.EventSequence_Event{
					Created: &testfixtures.BaseTime,
					Event: &armadaevents.EventSequence_Event_JobRunErrors{
						JobRunErrors: &armadaevents.JobRunErrors{
							JobId: testfixtures.JobIdProto,
							RunId: testfixtures.RunIdProto,
							Errors: []*armadaevents.Error{
								{
									Terminal: true,
									Reason: &armadaevents.Error_PodError{
										PodError: &armadaevents.PodError{
											Message:  "error message with null char \000",
											NodeName: testfixtures.NodeName,
											ContainerErrors: []*armadaevents.ContainerError{
												{ExitCode: 42},
											},
										},
									},
								},
							},
						},
					},
				})},
				MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
			},
			expected: &model.InstructionSet{
				JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
					RunId:       testfixtures.RunIdString,
					Node:        pointer.String(testfixtures.NodeName),
					Finished:    &testfixtures.BaseTime,
					JobRunState: pointer.Int32(lookout.JobRunFailedOrdinal),
					Error:       []byte("error message with null char "),
					ExitCode:    pointer.Int32(42),
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

func TestSubmitWithNullChar(t *testing.T) {
	submit := &armadaevents.EventSequence_Event{
		Created: &testfixtures.BaseTime,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:           testfixtures.JobIdProto,
				DeduplicationId: "",
				Priority:        0,
				ObjectMeta: &armadaevents.ObjectMeta{
					Namespace: testfixtures.Namespace,
				},
				MainObject: &armadaevents.KubernetesMainObject{
					Object: &armadaevents.KubernetesMainObject_PodSpec{
						PodSpec: &armadaevents.PodSpecWithAvoidList{
							PodSpec: &v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:    "container",
										Command: []string{"/bin/bash \000"},
										Args:    []string{"hello \000 world"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	converter := NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
	instructions := converter.Convert(context.Background(), &ingest.EventSequencesWithIds{
		EventSequences: []*armadaevents.EventSequence{testfixtures.NewEventSequence(submit)},
		MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	})
	assert.Len(t, instructions.JobsToCreate, 1)
	assert.NotContains(t, string(instructions.JobsToCreate[0].JobProto), "\\u0000")
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

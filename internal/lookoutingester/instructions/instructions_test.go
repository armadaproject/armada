package instructions

import (
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/G-Research/armada/internal/common/ingest"

	"github.com/G-Research/armada/internal/lookoutingester/metrics"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	f "github.com/G-Research/armada/internal/common/ingest/testfixtures"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Mock Pulsar Message with implementations only for the functions we care about
var (
	expectedApiJob, _      = eventutil.ApiJobFromLogSubmitJob(f.UserId, []string{}, f.Queue, f.JobSetName, f.BaseTime, f.Submit.GetSubmitJob())
	expectedApiJobProto, _ = proto.Marshal(expectedApiJob)
)

// Standard Set of expected rows for common tests
var expectedSubmit = model.CreateJobInstruction{
	JobId:     f.JobIdString,
	Queue:     f.Queue,
	Owner:     f.UserId,
	JobSet:    f.JobSetName,
	Priority:  f.Priority,
	Submitted: f.BaseTime,
	JobProto:  expectedApiJobProto,
	State:     repository.JobQueuedOrdinal,
	Updated:   f.BaseTime,
}

var expectedLeased = model.UpdateJobInstruction{
	JobId:   f.JobIdString,
	State:   pointer.Int32(repository.JobPendingOrdinal),
	Updated: f.BaseTime,
}

var expectedRunning = model.UpdateJobInstruction{
	JobId:   f.JobIdString,
	State:   pointer.Int32(repository.JobRunningOrdinal),
	Updated: f.BaseTime,
}

var expectedLeasedRun = model.CreateJobRunInstruction{
	RunId:   f.RunIdString,
	JobId:   f.JobIdString,
	Cluster: f.ExecutorId,
	Created: f.BaseTime,
}

var expectedRunningRun = model.UpdateJobRunInstruction{
	RunId:     f.RunIdString,
	Node:      pointer.String(f.NodeName),
	Started:   &f.BaseTime,
	PodNumber: pointer.Int32(f.PodNumber),
}

var expectedJobRunSucceeded = model.UpdateJobRunInstruction{
	RunId:     f.RunIdString,
	Finished:  &f.BaseTime,
	Succeeded: pointer.Bool(true),
}

var expectedJobSucceeded = model.UpdateJobInstruction{
	JobId:   f.JobIdString,
	State:   pointer.Int32(repository.JobSucceededOrdinal),
	Updated: f.BaseTime,
}

var expectedJobCancelled = model.UpdateJobInstruction{
	JobId:     f.JobIdString,
	Cancelled: &f.BaseTime,
	Updated:   f.BaseTime,
	State:     pointer.Int32(repository.JobCancelledOrdinal),
}

var expectedJobReprioritised = model.UpdateJobInstruction{
	JobId:    f.JobIdString,
	Priority: pointer.Int32(f.NewPriority),
	Updated:  f.BaseTime,
}

var expectedJobRunPreempted = model.UpdateJobRunInstruction{
	RunId:     f.RunIdString,
	Finished:  &f.BaseTime,
	Succeeded: pointer.Bool(false),
	Preempted: &f.BaseTime,
}

var expectedFailed = model.UpdateJobRunInstruction{
	RunId:     f.RunIdString,
	Node:      pointer.String(f.NodeName),
	Finished:  &f.BaseTime,
	Succeeded: pointer.Bool(false),
	Error:     pointer.String(f.ErrMsg),
}

var expectedJobRunContainer = model.CreateJobRunContainerInstruction{
	RunId:    f.RunIdString,
	ExitCode: 1,
}

// Single submit message
func TestSubmit(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.Submit)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

// Happy path of submit -> assigned -> running -> succeeded
// All in a single update
// Single submit message
func TestHappyPathSingleUpdate(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.Submit, f.Assigned, f.Running, f.JobRunSucceeded, f.JobSucceeded)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobsToCreate:    []*model.CreateJobInstruction{&expectedSubmit},
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedRunning, &expectedJobSucceeded},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun, &expectedJobRunSucceeded},
		MessageIds:      msg.MessageIds,
	}
	// assert each field separately as can be tricky to see what doesn't match
	assert.Equal(t, expected.JobsToCreate, instructions.JobsToCreate)
	assert.Equal(t, expected.JobsToUpdate, instructions.JobsToUpdate)
	assert.Equal(t, expected.JobRunsToCreate, instructions.JobRunsToCreate)
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
	assert.Equal(t, expected.MessageIds, instructions.MessageIds)
}

func TestHappyPathMultiUpdate(t *testing.T) {
	svc := SimpleInstructionConverter()
	// Submit
	msg1 := NewMsg(f.Submit)
	instructions := svc.Convert(context.Background(), msg1)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   msg1.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Leased
	msg2 := NewMsg(f.Assigned)
	instructions = svc.Convert(context.Background(), msg2)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		MessageIds:      msg2.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Running
	msg3 := NewMsg(f.Running)
	instructions = svc.Convert(context.Background(), msg3)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedRunning},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun},
		MessageIds:      msg3.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Run Succeeded
	msg4 := NewMsg(f.JobRunSucceeded)
	instructions = svc.Convert(context.Background(), msg4)
	expected = &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedJobRunSucceeded},
		MessageIds:      msg4.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Job Succeeded
	msg5 := NewMsg(f.JobSucceeded)
	instructions = svc.Convert(context.Background(), msg5)
	expected = &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobSucceeded},
		MessageIds:   msg5.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestCancelled(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.JobCancelled)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobCancelled},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestReprioritised(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.JobReprioritised)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobReprioritised},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestPreempted(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.JobPreempted)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedJobRunPreempted},
		MessageIds:      msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestFailed(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.JobRunFailed)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate:          []*model.UpdateJobRunInstruction{&expectedFailed},
		JobRunContainersToCreate: []*model.CreateJobRunContainerInstruction{&expectedJobRunContainer},
		MessageIds:               msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestFailedWithMissingRunId(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.JobLeaseReturned)
	instructions := svc.Convert(context.Background(), msg)
	jobRun := instructions.JobRunsToCreate[0]
	assert.NotEqual(t, eventutil.LEGACY_RUN_ID, jobRun.RunId)
	expected := &model.InstructionSet{
		JobRunsToCreate: []*model.CreateJobRunInstruction{
			{
				JobId:   f.JobIdString,
				RunId:   jobRun.RunId,
				Cluster: f.ExecutorId,
				Created: f.BaseTime,
			},
		},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{
			{
				RunId:            jobRun.RunId,
				Started:          &f.BaseTime,
				Finished:         &f.BaseTime,
				Succeeded:        pointer.Bool(false),
				Error:            pointer.String(f.LeaseReturnedMsg),
				UnableToSchedule: pointer.Bool(true),
			},
		},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
}

func TestHandlePodTerminated(t *testing.T) {
	terminatedMsg := "test pod terminated msg"

	podTerminated := &armadaevents.EventSequence_Event{
		Created: &f.BaseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: f.JobIdProto,
				RunId: f.RunIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodTerminated{
							PodTerminated: &armadaevents.PodTerminated{
								NodeName: f.NodeName,
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId: f.ExecutorId,
								},
								Message: terminatedMsg,
							},
						},
					},
				},
			},
		},
	}

	svc := SimpleInstructionConverter()
	msg := NewMsg(podTerminated)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:     f.RunIdString,
			Node:      pointer.String(f.NodeName),
			Finished:  &f.BaseTime,
			Succeeded: pointer.Bool(false),
			Error:     pointer.String(terminatedMsg),
		}},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestHandleJobLeaseReturned(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(f.LeaseReturned)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:            f.RunIdString,
			Finished:         &f.BaseTime,
			Succeeded:        pointer.Bool(false),
			Error:            pointer.String(f.LeaseReturnedMsg),
			UnableToSchedule: pointer.Bool(true),
		}},
		JobsToUpdate: []*model.UpdateJobInstruction{
			{
				JobId:   f.JobIdString,
				Updated: f.BaseTime,
				State:   pointer.Int32(repository.JobQueuedOrdinal),
			},
		},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestHandlePodUnschedulable(t *testing.T) {
	unschedulableMsg := "test pod unschedulable msg"

	podUnschedulable := &armadaevents.EventSequence_Event{
		Created: &f.BaseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: f.JobIdProto,
				RunId: f.RunIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodUnschedulable{
							PodUnschedulable: &armadaevents.PodUnschedulable{
								NodeName: f.NodeName,
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId: f.ExecutorId,
								},
								Message: unschedulableMsg,
							},
						},
					},
				},
			},
		},
	}

	svc := SimpleInstructionConverter()
	msg := NewMsg(podUnschedulable)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:            f.RunIdString,
			Node:             pointer.String(f.NodeName),
			Finished:         &f.BaseTime,
			Succeeded:        pointer.Bool(false),
			UnableToSchedule: pointer.Bool(true),
			Error:            pointer.String(unschedulableMsg),
		}},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestSubmitWithNullChar(t *testing.T) {
	msg := NewMsg(&armadaevents.EventSequence_Event{
		Created: &f.BaseTime,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:           f.JobIdProto,
				DeduplicationId: "",
				Priority:        0,
				ObjectMeta: &armadaevents.ObjectMeta{
					Namespace: f.Namespace,
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
	})

	svc := SimpleInstructionConverter()
	instructions := svc.Convert(context.Background(), msg)
	assert.Len(t, instructions.JobsToCreate, 1)
	assert.NotContains(t, string(instructions.JobsToCreate[0].JobProto), "\\u0000")
}

func TestFailedWithNullCharInError(t *testing.T) {
	msg := NewMsg(&armadaevents.EventSequence_Event{
		Created: &f.BaseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: f.JobIdProto,
				RunId: f.RunIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								Message:  "error message with null char \000",
								NodeName: f.NodeName,
								ContainerErrors: []*armadaevents.ContainerError{
									{ExitCode: 1},
								},
							},
						},
					},
				},
			},
		},
	})

	svc := SimpleInstructionConverter()
	instructions := svc.Convert(context.Background(), msg)
	expectedJobRunsToUpdate := []*model.UpdateJobRunInstruction{
		{
			RunId:     f.RunIdString,
			Finished:  &f.BaseTime,
			Succeeded: pointer.Bool(false),
			Node:      pointer.String(f.NodeName),
			Error:     pointer.String("error message with null char "),
		},
	}
	assert.Equal(t, expectedJobRunsToUpdate, instructions.JobRunsToUpdate)
}

func TestInvalidEvent(t *testing.T) {
	// This event is invalid as it doesn't have a job id or a run id
	invalidEvent := &armadaevents.EventSequence_Event{
		Created: &f.BaseTime,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{},
		},
	}

	// Check that the (valid) Submit is processed, but the invalid message is discarded
	svc := SimpleInstructionConverter()
	msg := NewMsg(invalidEvent, f.Submit)
	instructions := svc.Convert(context.Background(), msg)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestAnnotations(t *testing.T) {
	annotations := map[string]string{f.UserAnnotationPrefix + "a": "b", "1": "2"}
	expected := []*model.CreateUserAnnotationInstruction{
		{
			JobId: f.JobIdString,
			Key:   "1",
			Value: "2",
		},
		{
			JobId: f.JobIdString,
			Key:   "a",
			Value: "b",
		},
	}
	annotationInstructions := extractAnnotations(f.JobIdString, annotations, f.UserAnnotationPrefix)
	assert.Equal(t, expected, annotationInstructions)
}

func TestExtractNodeName(t *testing.T) {
	podError := armadaevents.PodError{}
	assert.Nil(t, extractNodeName(&podError))
	podError.NodeName = f.NodeName
	assert.Equal(t, pointer.String(f.NodeName), extractNodeName(&podError))
}

func NewMsg(event ...*armadaevents.EventSequence_Event) *ingest.EventSequencesWithIds {
	return &ingest.EventSequencesWithIds{
		EventSequences: []*armadaevents.EventSequence{f.NewEventSequence(event...)},
		MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}
}

func SimpleInstructionConverter() *InstructionConverter {
	return &InstructionConverter{
		metrics:              metrics.Get(),
		userAnnotationPrefix: f.UserAnnotationPrefix,
		compressor:           &compress.NoOpCompressor{},
	}
}

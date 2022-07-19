package instructions

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Mock Pulsar Message with implementations only for the functions we care about

//
// Standard Set of events for common tests
//

const jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
const runIdString = "123e4567-e89b-12d3-a456-426614174000"
const userAnnotationPrefix = "test_prefix/"

var jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
var runIdProto = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))

const jobSetName = "testJobset"
const executorId = "testCluster"
const nodeName = "testNode"
const podName = "test-pod"
const queue = "test-queue"
const userId = "testUser"
const namespace = "test-ns"
const priority = 3
const newPriority = 4
const podNumber = 6
const errMsg = "sample error message"
const leaseReturnedMsg = "lease returned error message"

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

// Submit
var submit = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_SubmitJob{
		SubmitJob: &armadaevents.SubmitJob{
			JobId:    jobIdProto,
			Priority: priority,
			ObjectMeta: &armadaevents.ObjectMeta{
				Namespace: namespace,
				Name:      "test-job",
			},
			MainObject: &armadaevents.KubernetesMainObject{
				Object: &armadaevents.KubernetesMainObject_PodSpec{
					PodSpec: &armadaevents.PodSpecWithAvoidList{
						PodSpec: &v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:    "container1",
									Image:   "alpine:latest",
									Command: []string{"myprogram.sh"},
									Args:    []string{"foo", "bar"},
									Resources: v1.ResourceRequirements{
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
				},
			},
		},
	},
}

// Assigned
var assigned = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobRunAssigned{
		JobRunAssigned: &armadaevents.JobRunAssigned{
			RunId: runIdProto,
			JobId: jobIdProto,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						KubernetesId: runIdString,
						Name:         podName,
						Namespace:    namespace,
						ExecutorId:   executorId,
					},
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							PodNumber: podNumber,
						},
					},
				},
			},
		},
	},
}

// Running
var running = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobRunRunning{
		JobRunRunning: &armadaevents.JobRunRunning{
			RunId: runIdProto,
			JobId: jobIdProto,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							NodeName:  nodeName,
							PodNumber: podNumber,
						},
					},
				},
			},
		},
	},
}

// Succeeded
var jobRunSucceeded = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: runIdProto,
			JobId: jobIdProto,
		},
	},
}

// Cancelled
var jobCancelled = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId: jobIdProto,
		},
	},
}

// Reprioritised
var jobReprioritised = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
		ReprioritisedJob: &armadaevents.ReprioritisedJob{
			JobId:    jobIdProto,
			Priority: newPriority,
		},
	},
}

// Job Run Failed
var jobRunFailed = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: jobIdProto,
			RunId: runIdProto,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodError{
						PodError: &armadaevents.PodError{
							Message:  errMsg,
							NodeName: nodeName,
							ContainerErrors: []*armadaevents.ContainerError{
								{ExitCode: 1},
							},
						},
					},
				},
			},
		},
	},
}

// Job Lease Returned
var jobLeaseReturned = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: jobIdProto,
			RunId: eventutil.LegacyJobRunId(),
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodLeaseReturned{
						PodLeaseReturned: &armadaevents.PodLeaseReturned{
							ObjectMeta: &armadaevents.ObjectMeta{
								ExecutorId: executorId,
							},
							Message: leaseReturnedMsg,
						},
					},
				},
			},
		},
	},
}

var jobSucceeded = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobSucceeded{
		JobSucceeded: &armadaevents.JobSucceeded{
			JobId: jobIdProto,
		},
	},
}

var expectedApiJob, _ = eventutil.ApiJobFromLogSubmitJob(userId, []string{}, queue, jobSetName, baseTime, submit.GetSubmitJob())
var expectedApiJobJson, _ = json.Marshal(expectedApiJob)
var expectedApiJobProto, _ = proto.Marshal(expectedApiJob)

//
// Standard Set of expected rows for common tests
//
var expectedSubmit = model.CreateJobInstruction{
	JobId:     jobIdString,
	Queue:     queue,
	Owner:     userId,
	JobSet:    jobSetName,
	Priority:  priority,
	Submitted: baseTime,
	JobJson:   expectedApiJobJson,
	JobProto:  expectedApiJobProto,
	State:     repository.JobQueuedOrdinal,
	Updated:   baseTime,
}

var expectedLeased = model.UpdateJobInstruction{
	JobId:   jobIdString,
	State:   pointer.Int32(repository.JobPendingOrdinal),
	Updated: baseTime,
}

var expectedRunning = model.UpdateJobInstruction{
	JobId:   jobIdString,
	State:   pointer.Int32(repository.JobRunningOrdinal),
	Updated: baseTime,
}

var expectedLeasedRun = model.CreateJobRunInstruction{
	RunId:   runIdString,
	JobId:   jobIdString,
	Cluster: executorId,
	Created: baseTime,
}

var expectedRunningRun = model.UpdateJobRunInstruction{
	RunId:     runIdString,
	Node:      pointer.String(nodeName),
	Started:   &baseTime,
	PodNumber: pointer.Int32(podNumber),
}

var expectedJobRunSucceeded = model.UpdateJobRunInstruction{
	RunId:     runIdString,
	Finished:  &baseTime,
	Succeeded: pointer.Bool(true),
}

var expectedJobSucceeded = model.UpdateJobInstruction{
	JobId:   jobIdString,
	State:   pointer.Int32(repository.JobSucceededOrdinal),
	Updated: baseTime,
}

var expectedJobCancelled = model.UpdateJobInstruction{
	JobId:     jobIdString,
	Cancelled: &baseTime,
	Updated:   baseTime,
	State:     pointer.Int32(repository.JobCancelledOrdinal),
}

var expectedJobReprioritised = model.UpdateJobInstruction{
	JobId:    jobIdString,
	Priority: pointer.Int32(newPriority),
	Updated:  baseTime,
}

var expectedFailed = model.UpdateJobRunInstruction{
	RunId:     runIdString,
	Node:      pointer.String(nodeName),
	Finished:  &baseTime,
	Succeeded: pointer.Bool(false),
	Error:     pointer.String(errMsg),
}

var expectedJobRunContainer = model.CreateJobRunContainerInstruction{
	RunId:    runIdString,
	ExitCode: 1,
}

// Single submit message
func TestSubmit(t *testing.T) {
	msg := NewMsg(baseTime, submit)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

// Happy path of submit -> assigned -> running -> succeeded
// All in a single update
// Single submit message
func TestHappyPathSingleUpdate(t *testing.T) {
	msg := NewMsg(baseTime, submit, assigned, running, jobRunSucceeded, jobSucceeded)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToCreate:    []*model.CreateJobInstruction{&expectedSubmit},
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedRunning, &expectedJobSucceeded},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun, &expectedJobRunSucceeded},
		MessageIds:      []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	// assert each field separately as can be tricky to see what doesn't match
	assert.Equal(t, expected.JobsToCreate, instructions.JobsToCreate)
	assert.Equal(t, expected.JobsToUpdate, instructions.JobsToUpdate)
	assert.Equal(t, expected.JobRunsToCreate, instructions.JobRunsToCreate)
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
	assert.Equal(t, expected.MessageIds, instructions.MessageIds)
}

func TestHappyPathMultiUpdate(t *testing.T) {

	compressor := &compress.NoOpCompressor{}

	// Submit
	msg1 := NewMsg(baseTime, submit)
	instructions := ConvertMsg(context.Background(), msg1, userAnnotationPrefix, compressor)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   []*pulsarutils.ConsumerMessageId{{msg1.Message.ID(), 0, msg1.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Leased
	msg2 := NewMsg(baseTime, assigned)
	instructions = ConvertMsg(context.Background(), msg2, userAnnotationPrefix, compressor)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		MessageIds:      []*pulsarutils.ConsumerMessageId{{msg2.Message.ID(), 0, msg2.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Running
	msg3 := NewMsg(baseTime, running)
	instructions = ConvertMsg(context.Background(), msg3, userAnnotationPrefix, compressor)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedRunning},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun},
		MessageIds:      []*pulsarutils.ConsumerMessageId{{msg3.Message.ID(), 0, msg3.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Run Suceeded
	msg4 := NewMsg(baseTime, jobRunSucceeded)
	instructions = ConvertMsg(context.Background(), msg4, userAnnotationPrefix, compressor)
	expected = &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedJobRunSucceeded},
		MessageIds:      []*pulsarutils.ConsumerMessageId{{msg4.Message.ID(), 0, msg4.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Job Suceeded
	msg5 := NewMsg(baseTime, jobSucceeded)
	instructions = ConvertMsg(context.Background(), msg5, userAnnotationPrefix, compressor)
	expected = &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobSucceeded},
		MessageIds:   []*pulsarutils.ConsumerMessageId{{msg5.Message.ID(), 0, msg5.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

}

func TestCancelled(t *testing.T) {
	msg := NewMsg(baseTime, jobCancelled)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobCancelled},
		MessageIds:   []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func TestReprioritised(t *testing.T) {
	msg := NewMsg(baseTime, jobReprioritised)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobReprioritised},
		MessageIds:   []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func TestFailed(t *testing.T) {
	msg := NewMsg(baseTime, jobRunFailed)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobRunsToUpdate:          []*model.UpdateJobRunInstruction{&expectedFailed},
		JobRunContainersToCreate: []*model.CreateJobRunContainerInstruction{&expectedJobRunContainer},
		MessageIds:               []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func TestFailedWithMissingRunId(t *testing.T) {
	msg := NewMsg(baseTime, jobLeaseReturned)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	jobRun := instructions.JobRunsToCreate[0]
	assert.NotEqual(t, eventutil.LEGACY_RUN_ID, jobRun.RunId)
	expected := &model.InstructionSet{
		JobRunsToCreate: []*model.CreateJobRunInstruction{
			{
				JobId:   jobIdString,
				RunId:   jobRun.RunId,
				Cluster: executorId,
				Created: baseTime,
			},
		},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{
			{
				RunId:            jobRun.RunId,
				Started:          &baseTime,
				Finished:         &baseTime,
				Succeeded:        pointer.Bool(false),
				Error:            pointer.String(leaseReturnedMsg),
				UnableToSchedule: pointer.Bool(true),
			},
		},
		MessageIds: []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
}

func TestHandlePodTerminated(t *testing.T) {

	terminatedMsg := "test pod terminated msg"

	podTerminated := &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodTerminated{
							PodTerminated: &armadaevents.PodTerminated{
								NodeName: nodeName,
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId: executorId,
								},
								Message: terminatedMsg,
							},
						},
					},
				},
			},
		},
	}

	msg := NewMsg(baseTime, podTerminated)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:     runIdString,
			Node:      pointer.String(nodeName),
			Finished:  &baseTime,
			Succeeded: pointer.Bool(false),
			Error:     pointer.String(terminatedMsg),
		}},
		MessageIds: []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

}

func TestHandlePodUnschedulable(t *testing.T) {

	unschedulableMsg := "test pod unschedulable msg"

	podUnschedulable := &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodUnschedulable{
							PodUnschedulable: &armadaevents.PodUnschedulable{
								NodeName: nodeName,
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId: executorId,
								},
								Message: unschedulableMsg,
							},
						},
					},
				},
			},
		},
	}

	msg := NewMsg(baseTime, podUnschedulable)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:            runIdString,
			Node:             pointer.String(nodeName),
			Finished:         &baseTime,
			Succeeded:        pointer.Bool(false),
			UnableToSchedule: pointer.Bool(true),
			Error:            pointer.String(unschedulableMsg),
		}},
		MessageIds: []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

}

func TestSubmitWithNullChar(t *testing.T) {
	msg := NewMsg(baseTime, &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:           jobIdProto,
				DeduplicationId: "",
				Priority:        0,
				ObjectMeta: &armadaevents.ObjectMeta{
					Namespace: namespace,
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
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	assert.Len(t, instructions.JobsToCreate, 1)
	assert.NotContains(t, string(instructions.JobsToCreate[0].JobJson), "\\u0000")
}

func TestFailedWithNullCharInError(t *testing.T) {
	msg := NewMsg(baseTime, &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								Message:  "error message with null char \000",
								NodeName: nodeName,
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
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expectedJobRunsToUpdate := []*model.UpdateJobRunInstruction{
		{
			RunId:     runIdString,
			Finished:  &baseTime,
			Succeeded: pointer.Bool(false),
			Node:      pointer.String(nodeName),
			Error:     pointer.String("error message with null char "),
		},
	}
	assert.Equal(t, expectedJobRunsToUpdate, instructions.JobRunsToUpdate)
}

func TestInvalidEvent(t *testing.T) {

	// This event is invalid as it doesn't have a job id or a run id
	invalidEvent := &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{},
		},
	}

	// Check that the (valid) Submit is processed, but the invalid message is discarded
	msg := NewMsg(baseTime, invalidEvent, submit)
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

// This message is invalid as it has no payload
// Assert that the update just contains the messageId so we can ack it
func TestInvalidMessage(t *testing.T) {
	msg := &pulsarutils.ConsumerMessage{Message: pulsarutils.EmptyPulsarMessage(3, time.Now()), ConsumerId: 3}
	instructions := ConvertMsg(context.Background(), msg, userAnnotationPrefix, &compress.NoOpCompressor{})
	expected := &model.InstructionSet{
		MessageIds: []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func TestAnnotations(t *testing.T) {
	annotations := map[string]string{userAnnotationPrefix + "a": "b", "1": "2"}
	expected := []*model.CreateUserAnnotationInstruction{
		{
			JobId: jobIdString,
			Key:   "1",
			Value: "2",
		},
		{
			JobId: jobIdString,
			Key:   "a",
			Value: "b",
		}}
	annotationInstructions := extractAnnotations(jobIdString, annotations, userAnnotationPrefix)
	assert.Equal(t, expected, annotationInstructions)
}

func NewMsg(publishTime time.Time, event ...*armadaevents.EventSequence_Event) *pulsarutils.ConsumerMessage {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events:     event,
		UserId:     userId,
	}
	payload, _ := proto.Marshal(seq)
	messageSeq := rand.Int()
	return &pulsarutils.ConsumerMessage{Message: pulsarutils.NewPulsarMessage(messageSeq, publishTime, payload), ConsumerId: messageSeq}
}

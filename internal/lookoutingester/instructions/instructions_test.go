package instructions

import (
	"encoding/json"
	"golang.org/x/net/context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/internal/lookoutingester/testutil"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Mock Pulsar Message with implementations only for the functions we care about

//
// Standard Set of events for common tests
//

const jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
const runIdString = "123e4567-e89b-12d3-a456-426614174000"

var jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
var runIdProto = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))

const jobSetName = "testJobset"
const executorId = "testCluster"
const nodeName = "testNode"
const queue = "test-queue"
const userId = "testUser"
const priority = 3
const newPriority = 4
const podNumber = 6

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

// Submit
var submit = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_SubmitJob{
		SubmitJob: &armadaevents.SubmitJob{
			JobId:    jobIdProto,
			Priority: priority,
			ObjectMeta: &armadaevents.ObjectMeta{
				Namespace: "test-ns",
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

// Leased
var leased = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_JobRunLeased{
		JobRunLeased: &armadaevents.JobRunLeased{
			RunId:      runIdProto,
			JobId:      jobIdProto,
			ExecutorId: executorId,
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

// Single submit message
func TestSubmit(t *testing.T) {
	msg := NewMsg(baseTime, submit)
	instructions := ConvertMsg(context.Background(), msg, &NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   []*model.ConsumerMessageId{{msg.Message.ID(), msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

// Happy path of submit -> leased -> running -> succeeded
// All in a single update
// Single submit message
func TestHappyPathSingleUpdate(t *testing.T) {
	msg := NewMsg(baseTime, submit, leased, running, jobRunSucceeded, jobSucceeded)
	instructions := ConvertMsg(context.Background(), msg, &NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToCreate:    []*model.CreateJobInstruction{&expectedSubmit},
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased, &expectedRunning, &expectedJobSucceeded},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun, &expectedJobRunSucceeded},
		MessageIds:      []*model.ConsumerMessageId{{msg.Message.ID(), msg.ConsumerId}},
	}
	// assert each field separately as can be tricky to see what doesn't match
	assert.Equal(t, expected.JobsToCreate, instructions.JobsToCreate)
	assert.Equal(t, expected.JobsToUpdate, instructions.JobsToUpdate)
	assert.Equal(t, expected.JobRunsToCreate, instructions.JobRunsToCreate)
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
	assert.Equal(t, expected.MessageIds, instructions.MessageIds)
}

func TestHappyPathMultiUpdate(t *testing.T) {

	compressor := &NoOpCompressor{}

	// Submit
	msg1 := NewMsg(baseTime, submit)
	instructions := ConvertMsg(context.Background(), msg1, compressor)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   []*model.ConsumerMessageId{{msg1.Message.ID(), msg1.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Leased
	msg2 := NewMsg(baseTime, leased)
	instructions = ConvertMsg(context.Background(), msg2, compressor)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		MessageIds:      []*model.ConsumerMessageId{{msg2.Message.ID(), msg2.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Running
	msg3 := NewMsg(baseTime, running)
	instructions = ConvertMsg(context.Background(), msg3, compressor)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedRunning},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun},
		MessageIds:      []*model.ConsumerMessageId{{msg3.Message.ID(), msg3.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Run Suceeded
	msg4 := NewMsg(baseTime, jobRunSucceeded)
	instructions = ConvertMsg(context.Background(), msg4, compressor)
	expected = &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedJobRunSucceeded},
		MessageIds:      []*model.ConsumerMessageId{{msg4.Message.ID(), msg4.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

	// Job Suceeded
	msg5 := NewMsg(baseTime, jobSucceeded)
	instructions = ConvertMsg(context.Background(), msg5, compressor)
	expected = &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobSucceeded},
		MessageIds:   []*model.ConsumerMessageId{{msg5.Message.ID(), msg5.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)

}

func TestCancelled(t *testing.T) {
	msg := NewMsg(baseTime, jobCancelled)
	instructions := ConvertMsg(context.Background(), msg, &NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobCancelled},
		MessageIds:   []*model.ConsumerMessageId{{msg.Message.ID(), msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func TestReprioritised(t *testing.T) {
	msg := NewMsg(baseTime, jobReprioritised)
	instructions := ConvertMsg(context.Background(), msg, &NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobReprioritised},
		MessageIds:   []*model.ConsumerMessageId{{msg.Message.ID(), msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func TestInvalidEvent(t *testing.T) {

	// This event is invalid as it references a job that doesn't exist
	nonExistingJob, _ := armadaevents.ProtoUuidFromUlidString("01f3j0g1md6qx7z5qb148qnh4r")
	invalidEvent := &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{
				JobId:      nonExistingJob,
				ExecutorId: executorId,
			},
		},
	}

	// Check that the (valid) Submit is processed, but the invalid message is discarded
	msg := NewMsg(baseTime, invalidEvent, submit)
	instructions := ConvertMsg(context.Background(), msg, &NoOpCompressor{})
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   []*model.ConsumerMessageId{{msg.Message.ID(), msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

// This message is invalid as it has no payload
// Assert that the update just contains the messageId so we can ack it
func TestInvalidMessage(t *testing.T) {
	msg := &model.ConsumerMessage{Message: testutil.EmptyPulsarMessage(3, time.Now()), ConsumerId: 3}
	instructions := ConvertMsg(context.Background(), msg, &NoOpCompressor{})
	expected := &model.InstructionSet{
		MessageIds: []*model.ConsumerMessageId{{msg.Message.ID(), msg.ConsumerId}},
	}
	assert.Equal(t, expected, instructions)
}

func NewMsg(publishTime time.Time, event ...*armadaevents.EventSequence_Event) *model.ConsumerMessage {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events:     event,
		UserId:     userId,
	}
	payload, _ := proto.Marshal(seq)
	messageSeq := rand.Int()
	return &model.ConsumerMessage{Message: testutil.NewPulsarMessage(messageSeq, publishTime, payload), ConsumerId: messageSeq}
}

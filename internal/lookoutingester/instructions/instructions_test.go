package instructions

import (
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Mock Pulsar Message with implementations only for the functions we care about

//
// Standard Set of events for common tests
//

const (
	jobIdString          = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString          = "123e4567-e89b-12d3-a456-426614174000"
	userAnnotationPrefix = "test_prefix/"
)

var (
	jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
	runIdProto    = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))
)

const (
	jobSetName       = "testJobset"
	executorId       = "testCluster"
	nodeName         = "testNode"
	podName          = "test-pod"
	queue            = "test-queue"
	userId           = "testUser"
	namespace        = "test-ns"
	priority         = 3
	newPriority      = 4
	podNumber        = 6
	errMsg           = "sample error message"
	leaseReturnedMsg = "lease returned error message"
)

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

// Submit
var submit = &armadaevents.EventSequence_Event{
	Created: &baseTime,
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
	Created: &baseTime,
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
	Created: &baseTime,
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
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: runIdProto,
			JobId: jobIdProto,
		},
	},
}

// Cancelled
var jobCancelled = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId: jobIdProto,
		},
	},
}

// Reprioritised
var jobReprioritised = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
		ReprioritisedJob: &armadaevents.ReprioritisedJob{
			JobId:    jobIdProto,
			Priority: newPriority,
		},
	},
}

// Preempted
var jobPreempted = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobRunPreempted{
		JobRunPreempted: &armadaevents.JobRunPreempted{
			PreemptedRunId: runIdProto,
		},
	},
}

// Job Run Failed
var jobRunFailed = &armadaevents.EventSequence_Event{
	Created: &baseTime,
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
	Created: &baseTime,
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
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobSucceeded{
		JobSucceeded: &armadaevents.JobSucceeded{
			JobId: jobIdProto,
		},
	},
}

var (
	expectedApiJob, _      = eventutil.ApiJobFromLogSubmitJob(userId, []string{}, queue, jobSetName, baseTime, submit.GetSubmitJob())
	expectedApiJobProto, _ = proto.Marshal(expectedApiJob)
)

// Standard Set of expected rows for common tests
var expectedSubmit = model.CreateJobInstruction{
	JobId:     jobIdString,
	Queue:     queue,
	Owner:     userId,
	JobSet:    jobSetName,
	Priority:  priority,
	Submitted: baseTime,
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

var expectedJobRunPreempted = model.UpdateJobRunInstruction{
	RunId:     runIdString,
	Finished:  &baseTime,
	Succeeded: pointer.Bool(false),
	Preempted: &baseTime,
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
	svc := SimpleInstructionConverter()
	msg := NewMsg(submit)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

// Single duplicate submit message is ignored
func TestDuplicate(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(testfixtures.SubmitDuplicate)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

// Happy path of submit -> assigned -> running -> succeeded
// All in a single update
// Single submit message
func TestHappyPathSingleUpdate(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(submit, assigned, running, jobRunSucceeded, jobSucceeded)
	instructions := svc.Convert(armadacontext.Background(), msg)
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
	msg1 := NewMsg(submit)
	instructions := svc.Convert(armadacontext.Background(), msg1)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   msg1.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Leased
	msg2 := NewMsg(assigned)
	instructions = svc.Convert(armadacontext.Background(), msg2)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedLeased},
		JobRunsToCreate: []*model.CreateJobRunInstruction{&expectedLeasedRun},
		MessageIds:      msg2.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Running
	msg3 := NewMsg(running)
	instructions = svc.Convert(armadacontext.Background(), msg3)
	expected = &model.InstructionSet{
		JobsToUpdate:    []*model.UpdateJobInstruction{&expectedRunning},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedRunningRun},
		MessageIds:      msg3.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Run Succeeded
	msg4 := NewMsg(jobRunSucceeded)
	instructions = svc.Convert(armadacontext.Background(), msg4)
	expected = &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedJobRunSucceeded},
		MessageIds:      msg4.MessageIds,
	}
	assert.Equal(t, expected, instructions)

	// Job Succeeded
	msg5 := NewMsg(jobSucceeded)
	instructions = svc.Convert(armadacontext.Background(), msg5)
	expected = &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobSucceeded},
		MessageIds:   msg5.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestCancelled(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(jobCancelled)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobCancelled},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestReprioritised(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(jobReprioritised)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{&expectedJobReprioritised},
		MessageIds:   msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestPreempted(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(jobPreempted)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{&expectedJobRunPreempted},
		MessageIds:      msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestFailed(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(jobRunFailed)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate:          []*model.UpdateJobRunInstruction{&expectedFailed},
		JobRunContainersToCreate: []*model.CreateJobRunContainerInstruction{&expectedJobRunContainer},
		MessageIds:               msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestFailedWithMissingRunId(t *testing.T) {
	svc := SimpleInstructionConverter()
	msg := NewMsg(jobLeaseReturned)
	instructions := svc.Convert(armadacontext.Background(), msg)
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
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected.JobRunsToUpdate, instructions.JobRunsToUpdate)
}

func TestHandlePodTerminated(t *testing.T) {
	terminatedMsg := "test pod terminated msg"

	podTerminated := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: false,
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

	svc := SimpleInstructionConverter()
	msg := NewMsg(podTerminated)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestHandleJobLeaseReturned(t *testing.T) {
	leaseReturnedMsg := "test pod returned msg"
	leaseReturned := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodLeaseReturned{
							PodLeaseReturned: &armadaevents.PodLeaseReturned{
								Message: leaseReturnedMsg,
							},
						},
					},
				},
			},
		},
	}

	svc := SimpleInstructionConverter()
	msg := NewMsg(leaseReturned)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:            runIdString,
			Finished:         &baseTime,
			Succeeded:        pointer.Bool(false),
			Error:            pointer.String(leaseReturnedMsg),
			UnableToSchedule: pointer.Bool(true),
		}},
		// TODO: re-enable this once the executor is fixed to no longer send spurious lease returned messages
		//JobsToUpdate: []*model.UpdateJobInstruction{
		//	{
		//		JobId:   jobIdString,
		//		Updated: baseTime,
		//		State:   pointer.Int32(repository.JobQueuedOrdinal),
		//	},
		//},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestHandlePodUnschedulable(t *testing.T) {
	unschedulableMsg := "test pod unschedulable msg"

	podUnschedulable := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: false,
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

	svc := SimpleInstructionConverter()
	msg := NewMsg(podUnschedulable)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId: runIdString,
			Node:  pointer.String(nodeName),
		}},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestHandleDuplicate(t *testing.T) {
	duplicate := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobDuplicateDetected{
			JobDuplicateDetected: &armadaevents.JobDuplicateDetected{
				NewJobId: jobIdProto,
			},
		},
	}

	svc := SimpleInstructionConverter()
	msg := NewMsg(duplicate)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobsToUpdate: []*model.UpdateJobInstruction{
			{
				JobId:     jobIdString,
				State:     pointer.Int32(repository.JobDuplicateOrdinal),
				Updated:   baseTime,
				Duplicate: pointer.Bool(true),
			},
		},
		MessageIds: msg.MessageIds,
	}
	assert.Equal(t, expected, instructions)
}

func TestSubmitWithNullChar(t *testing.T) {
	msg := NewMsg(&armadaevents.EventSequence_Event{
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

	svc := SimpleInstructionConverter()
	instructions := svc.Convert(armadacontext.Background(), msg)
	assert.Len(t, instructions.JobsToCreate, 1)
	assert.NotContains(t, string(instructions.JobsToCreate[0].JobProto), "\\u0000")
}

func TestFailedWithNullCharInError(t *testing.T) {
	msg := NewMsg(&armadaevents.EventSequence_Event{
		Created: &baseTime,
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

	svc := SimpleInstructionConverter()
	instructions := svc.Convert(armadacontext.Background(), msg)
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
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{},
		},
	}

	// Check that the (valid) Submit is processed, but the invalid message is discarded
	svc := SimpleInstructionConverter()
	msg := NewMsg(invalidEvent, submit)
	instructions := svc.Convert(armadacontext.Background(), msg)
	expected := &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{&expectedSubmit},
		MessageIds:   msg.MessageIds,
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
		},
	}
	annotationInstructions := extractAnnotations(jobIdString, annotations, userAnnotationPrefix)
	assert.Equal(t, expected, annotationInstructions)
}

func TestExtractNodeName(t *testing.T) {
	podError := armadaevents.PodError{}
	assert.Nil(t, extractNodeName(&podError))
	podError.NodeName = nodeName
	assert.Equal(t, pointer.String(nodeName), extractNodeName(&podError))
}

func NewMsg(event ...*armadaevents.EventSequence_Event) *ingest.EventSequencesWithIds {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events:     event,
		UserId:     userId,
	}
	return &ingest.EventSequencesWithIds{
		EventSequences: []*armadaevents.EventSequence{seq},
		MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}
}

func SimpleInstructionConverter() *InstructionConverter {
	return &InstructionConverter{
		metrics:              metrics.Get(),
		userAnnotationPrefix: userAnnotationPrefix,
		compressor:           &compress.NoOpCompressor{},
	}
}

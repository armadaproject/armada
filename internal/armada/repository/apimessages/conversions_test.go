package apimessages

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/require"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	v11 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobIdString           = "01f3j0g1md4qx7z5qb148qnh4r"
	preemptiveJobIdString = "02f3j0g1md4qx7z5qb148qnh4r"
	runIdString           = "123e4567-e89b-12d3-a456-426614174000"
	preemptiveRunIdString = "123e4567-e89b-12d3-a456-426614174001"
)

var (
	jobIdProto, _             = armadaevents.ProtoUuidFromUlidString(jobIdString)
	runIdProto                = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))
	preemptiveJobIdProto, _   = armadaevents.ProtoUuidFromUlidString(preemptiveJobIdString)
	preemptiveRunIdRunIdProto = armadaevents.ProtoUuidFromUuid(uuid.MustParse(preemptiveRunIdString))
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

func TestConvertSubmitted(t *testing.T) {
	// Submit
	submit := &armadaevents.EventSequence_Event{
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
										Name: "container1",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Submitted{
				Submitted: &api.JobSubmittedEvent{
					JobId:    jobIdString,
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  baseTime,
					Job: api.Job{
						Id:         jobIdString,
						JobSetId:   jobSetName,
						Queue:      queue,
						Namespace:  namespace,
						Owner:      userId,
						Priority:   priority,
						Created:    baseTime,
						K8SIngress: []*v11.Ingress{},
						K8SService: []*v1.Service{},
						PodSpec: &v1.PodSpec{
							Containers: []v1.Container{
								{
									Name: "container1",
								},
							},
						},
					},
				},
			},
		},
		{
			Events: &api.EventMessage_Queued{Queued: &api.JobQueuedEvent{
				JobId:    jobIdString,
				JobSetId: jobSetName,
				Queue:    queue,
				Created:  baseTime,
			}},
		},
	}
	apiEvents, err := FromEventSequence(toEventSeq(submit))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertCancel(t *testing.T) {
	cancel := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_CancelJob{
			CancelJob: &armadaevents.CancelJob{
				JobId: jobIdProto,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Cancelling{
				Cancelling: &api.JobCancellingEvent{
					JobId:     jobIdString,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   baseTime,
					Requestor: userId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(cancel))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertCancelled(t *testing.T) {
	cancel := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_CancelledJob{
			CancelledJob: &armadaevents.CancelledJob{
				JobId: jobIdProto,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Cancelled{
				Cancelled: &api.JobCancelledEvent{
					JobId:     jobIdString,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   baseTime,
					Requestor: userId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(cancel))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertReprioritising(t *testing.T) {
	reprioritising := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
			ReprioritiseJob: &armadaevents.ReprioritiseJob{
				JobId: jobIdProto,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Reprioritizing{
				Reprioritizing: &api.JobReprioritizingEvent{
					JobId:     jobIdString,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   baseTime,
					Requestor: userId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(reprioritising))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertReprioritised(t *testing.T) {
	reprioritised := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
			ReprioritisedJob: &armadaevents.ReprioritisedJob{
				JobId: jobIdProto,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Reprioritized{
				Reprioritized: &api.JobReprioritizedEvent{
					JobId:     jobIdString,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   baseTime,
					Requestor: userId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(reprioritised))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestDuplicateJob(t *testing.T) {
	oldJobString := "02f3j0g1md4qx7z5qb148qnh4r"
	oldJobProto, _ := armadaevents.ProtoUuidFromUlidString(oldJobString)

	duplicate := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobDuplicateDetected{
			JobDuplicateDetected: &armadaevents.JobDuplicateDetected{
				NewJobId: jobIdProto,
				OldJobId: oldJobProto,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_DuplicateFound{
				DuplicateFound: &api.JobDuplicateFoundEvent{
					JobId:         jobIdString,
					JobSetId:      jobSetName,
					Queue:         queue,
					Created:       baseTime,
					OriginalJobId: oldJobString,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(duplicate))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertLeased(t *testing.T) {
	leased := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{
				JobId:      jobIdProto,
				ExecutorId: executorId,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Leased{
				Leased: &api.JobLeasedEvent{
					JobId:     jobIdString,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   baseTime,
					ClusterId: executorId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(leased))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertLeaseExpired(t *testing.T) {
	leaseExpired := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_LeaseExpired{
							LeaseExpired: &armadaevents.LeaseExpired{},
						},
					},
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_LeaseExpired{
				LeaseExpired: &api.JobLeaseExpiredEvent{
					JobId:    jobIdString,
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  baseTime,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(leaseExpired))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertPodUnschedulable(t *testing.T) {
	unschedulable := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodUnschedulable{
							PodUnschedulable: &armadaevents.PodUnschedulable{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runIdString,
								},
								Message:   "couldn't schedule pod",
								NodeName:  nodeName,
								PodNumber: podNumber,
							},
						},
					},
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_UnableToSchedule{
				UnableToSchedule: &api.JobUnableToScheduleEvent{
					JobId:        jobIdString,
					ClusterId:    executorId,
					PodNamespace: namespace,
					PodName:      podName,
					KubernetesId: runIdString,
					Reason:       "couldn't schedule pod",
					NodeName:     nodeName,
					PodNumber:    podNumber,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(unschedulable))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertPodLeaseReturned(t *testing.T) {
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
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runIdString,
								},
								Message:      "couldn't schedule pod",
								PodNumber:    podNumber,
								RunAttempted: true,
							},
						},
					},
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_LeaseReturned{
				LeaseReturned: &api.JobLeaseReturnedEvent{
					JobId:        jobIdString,
					ClusterId:    executorId,
					KubernetesId: runIdString,
					Reason:       "couldn't schedule pod",
					PodNumber:    podNumber,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					RunAttempted: true,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(leaseReturned))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertPodTerminated(t *testing.T) {
	terminated := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodTerminated{
							PodTerminated: &armadaevents.PodTerminated{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runIdString,
								},
								Message:   "The pod was terminated",
								NodeName:  nodeName,
								PodNumber: podNumber,
							},
						},
					},
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Terminated{
				Terminated: &api.JobTerminatedEvent{
					JobId:        jobIdString,
					ClusterId:    executorId,
					PodNamespace: namespace,
					PodName:      podName,
					KubernetesId: runIdString,
					Reason:       "The pod was terminated",
					PodNumber:    podNumber,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(terminated))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestWeirdMarshaling(t *testing.T) {
	// create an object
	containerError := &armadaevents.ContainerError{
		Message:          "test error",
		KubernetesReason: &armadaevents.ContainerError_OutOfMemory_{},
	}

	// marshall it
	bytes, err := proto.Marshal(containerError)
	require.NoError(t, err)

	// unmarshall it
	unmarshalledContainerError := &armadaevents.ContainerError{}
	err = proto.Unmarshal(bytes, unmarshalledContainerError)
	require.NoError(t, err)

	require.Equal(t, containerError.Message, unmarshalledContainerError.Message)                   // passes
	require.Equal(t, containerError.KubernetesReason, unmarshalledContainerError.KubernetesReason) // fails wtf?!
}

func TestConvertJobError(t *testing.T) {
	errored := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobErrors{
			JobErrors: &armadaevents.JobErrors{
				JobId: jobIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runIdString,
								},
								Message:   "The pod was terminated",
								NodeName:  nodeName,
								PodNumber: podNumber,
								ContainerErrors: []*armadaevents.ContainerError{
									{
										ObjectMeta: &armadaevents.ObjectMeta{
											Name: "container1",
										},
										ExitCode:         -1,
										Message:          "container1 Error",
										Reason:           "container1 Reason",
										KubernetesReason: &armadaevents.ContainerError_OutOfMemory_{},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Failed{
				Failed: &api.JobFailedEvent{
					JobId:        jobIdString,
					ClusterId:    executorId,
					PodNamespace: namespace,
					PodName:      podName,
					NodeName:     nodeName,
					KubernetesId: runIdString,
					Reason:       "The pod was terminated",
					PodNumber:    podNumber,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					ContainerStatuses: []*api.ContainerStatus{
						{
							Name:     "container1",
							ExitCode: -1,
							Message:  "container1 Error",
							Reason:   "container1 Reason",
							Cause:    api.Cause_OOM,
						},
					},
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(errored))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobSucceeded(t *testing.T) {
	succeeded := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobSucceeded{
			JobSucceeded: &armadaevents.JobSucceeded{
				JobId: jobIdProto,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   executorId,
							Namespace:    namespace,
							Name:         podName,
							KubernetesId: runIdString,
						},
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

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Succeeded{
				Succeeded: &api.JobSucceededEvent{
					JobId:        jobIdString,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					ClusterId:    executorId,
					KubernetesId: runIdString,
					NodeName:     nodeName,
					PodNumber:    podNumber,
					PodName:      podName,
					PodNamespace: namespace,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(succeeded))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobRunning(t *testing.T) {
	running := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{
				RunId: runIdProto,
				JobId: jobIdProto,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   executorId,
							Namespace:    namespace,
							Name:         podName,
							KubernetesId: runIdString,
						},
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

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Running{
				Running: &api.JobRunningEvent{
					JobId:        jobIdString,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					ClusterId:    executorId,
					KubernetesId: runIdString,
					NodeName:     nodeName,
					PodNumber:    podNumber,
					PodName:      podName,
					PodNamespace: namespace,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(running))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestIgnoredEventDoesntDuplicate(t *testing.T) {
	leaseExpired := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: jobIdProto,
				RunId: runIdProto,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_LeaseExpired{
							LeaseExpired: &armadaevents.LeaseExpired{},
						},
					},
				},
			},
		},
	}

	cancel := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_CancelJobSet{
			CancelJobSet: &armadaevents.CancelJobSet{},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_LeaseExpired{
				LeaseExpired: &api.JobLeaseExpiredEvent{
					JobId:    jobIdString,
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  baseTime,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(leaseExpired, cancel))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobAssigned(t *testing.T) {
	running := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunAssigned{
			JobRunAssigned: &armadaevents.JobRunAssigned{
				RunId: runIdProto,
				JobId: jobIdProto,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   executorId,
							Namespace:    namespace,
							Name:         podName,
							KubernetesId: runIdString,
						},
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

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Pending{
				Pending: &api.JobPendingEvent{
					JobId:        jobIdString,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					ClusterId:    executorId,
					KubernetesId: runIdString,
					PodNumber:    podNumber,
					PodName:      podName,
					PodNamespace: namespace,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(running))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertResourceUtilisation(t *testing.T) {
	utilisation := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_ResourceUtilisation{
			ResourceUtilisation: &armadaevents.ResourceUtilisation{
				RunId: runIdProto,
				JobId: jobIdProto,
				ResourceInfo: &armadaevents.KubernetesResourceInfo{
					ObjectMeta: &armadaevents.ObjectMeta{
						ExecutorId:   executorId,
						Namespace:    namespace,
						Name:         podName,
						KubernetesId: runIdString,
					},
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							NodeName:  nodeName,
							PodNumber: podNumber,
						},
					},
				},
				MaxResourcesForPeriod: map[string]resource.Quantity{
					"cpu": resource.MustParse("2.0"),
					"mem": resource.MustParse("100Gi"),
				},
				TotalCumulativeUsage: map[string]resource.Quantity{
					"cpu": resource.MustParse("3.0"),
					"mem": resource.MustParse("200Gi"),
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Utilisation{
				Utilisation: &api.JobUtilisationEvent{
					JobId:        jobIdString,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					ClusterId:    executorId,
					KubernetesId: runIdString,
					MaxResourcesForPeriod: map[string]resource.Quantity{
						"cpu": resource.MustParse("2.0"),
						"mem": resource.MustParse("100Gi"),
					},
					NodeName:     nodeName,
					PodNumber:    podNumber,
					PodName:      podName,
					PodNamespace: namespace,
					TotalCumulativeUsage: map[string]resource.Quantity{
						"cpu": resource.MustParse("3.0"),
						"mem": resource.MustParse("200Gi"),
					},
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(utilisation))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertIngressInfo(t *testing.T) {
	utilisation := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_StandaloneIngressInfo{
			StandaloneIngressInfo: &armadaevents.StandaloneIngressInfo{
				RunId: runIdProto,
				JobId: jobIdProto,
				ObjectMeta: &armadaevents.ObjectMeta{
					ExecutorId:   executorId,
					Namespace:    namespace,
					Name:         podName,
					KubernetesId: runIdString,
				},
				IngressAddresses: map[int32]string{
					1: "http://som-ingress:80",
				},
				NodeName:     nodeName,
				PodNumber:    podNumber,
				PodName:      podName,
				PodNamespace: namespace,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_IngressInfo{
				IngressInfo: &api.JobIngressInfoEvent{
					JobId:        jobIdString,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      baseTime,
					ClusterId:    executorId,
					KubernetesId: runIdString,
					NodeName:     nodeName,
					PodNumber:    podNumber,
					PodName:      podName,
					PodNamespace: namespace,
					IngressAddresses: map[int32]string{
						1: "http://som-ingress:80",
					},
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(utilisation))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobRunPreempted(t *testing.T) {
	preempted := &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{
				PreemptedJobId:  jobIdProto,
				PreemptedRunId:  runIdProto,
				PreemptiveJobId: preemptiveJobIdProto,
				PreemptiveRunId: preemptiveRunIdRunIdProto,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Preempted{
				Preempted: &api.JobPreemptedEvent{
					JobId:           jobIdString,
					JobSetId:        jobSetName,
					Queue:           queue,
					Created:         baseTime,
					RunId:           runIdString,
					PreemptiveJobId: preemptiveJobIdString,
					PreemptiveRunId: preemptiveRunIdString,
				},
			},
		},
	}

	// both PreemptiveJobId and PreemptiveRunId not nil
	apiEvents, err := FromEventSequence(toEventSeq(preempted))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)

	// PreemptiveJobId is nil
	preemptiveJobIdNil := proto.Clone(preempted).(*armadaevents.EventSequence_Event)
	preemptiveJobIdNil.GetJobRunPreempted().PreemptiveJobId = nil

	expectedPreemptiveJobIdNil := proto.Clone(expected[0]).(*api.EventMessage)
	expectedPreemptiveJobIdNil.GetPreempted().PreemptiveJobId = ""
	apiEvents, err = FromEventSequence(toEventSeq(preemptiveJobIdNil))
	assert.NoError(t, err)
	assert.Equal(t, []*api.EventMessage{expectedPreemptiveJobIdNil}, apiEvents)

	// PreemptiveRunId is nil
	preemptiveRunIdNil := proto.Clone(preempted).(*armadaevents.EventSequence_Event)
	preemptiveRunIdNil.GetJobRunPreempted().PreemptiveRunId = nil

	expectedPreemptiveRunIdNil := proto.Clone(expected[0]).(*api.EventMessage)
	expectedPreemptiveRunIdNil.GetPreempted().PreemptiveRunId = ""
	apiEvents, err = FromEventSequence(toEventSeq(preemptiveRunIdNil))
	assert.NoError(t, err)
	assert.Equal(t, []*api.EventMessage{expectedPreemptiveRunIdNil}, apiEvents)
}

func toEventSeq(event ...*armadaevents.EventSequence_Event) *armadaevents.EventSequence {
	return &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events:     event,
		UserId:     userId,
	}
}

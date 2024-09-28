package conversion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	v11 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString = "123e4567-e89b-12d3-a456-426614174000"
)

var (
	baseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	baseTimeProto = protoutil.ToTimestamp(baseTime)
)

const (
	jobSetName = "testJobset"
	executorId = "testCluster"
	nodeName   = "testNode"
	podName    = "test-pod"
	queue      = "test-queue"
	userId     = "testUser"
	namespace  = "test-ns"
	priority   = 3
	podNumber  = 6
)

func TestConvertSubmitted(t *testing.T) {
	// Submit
	submit := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobIdStr: jobIdString,
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
					Created:  protoutil.ToTimestamp(baseTime),
					Job: &api.Job{
						Id:         jobIdString,
						JobSetId:   jobSetName,
						Queue:      queue,
						Namespace:  namespace,
						Owner:      userId,
						Priority:   priority,
						Created:    protoutil.ToTimestamp(baseTime),
						K8SIngress: []*v11.Ingress{},
						K8SService: []*v1.Service{},
						PodSpec: &v1.PodSpec{
							Containers: []v1.Container{
								{
									Name: "container1",
								},
							},
						},
						SchedulingResourceRequirements: &v1.ResourceRequirements{
							Requests: make(v1.ResourceList),
							Limits:   make(v1.ResourceList),
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
				Created:  protoutil.ToTimestamp(baseTime),
			}},
		},
	}
	apiEvents, err := FromEventSequence(toEventSeq(submit))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertCancel(t *testing.T) {
	cancel := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_CancelJob{
			CancelJob: &armadaevents.CancelJob{
				JobIdStr: jobIdString,
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
					Created:   protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_CancelledJob{
			CancelledJob: &armadaevents.CancelledJob{
				JobIdStr: jobIdString,
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
					Created:   protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
			ReprioritiseJob: &armadaevents.ReprioritiseJob{
				JobIdStr: jobIdString,
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
					Created:   protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
			ReprioritisedJob: &armadaevents.ReprioritisedJob{
				JobIdStr: jobIdString,
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
					Created:   protoutil.ToTimestamp(baseTime),
					Requestor: userId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(reprioritised))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertLeased(t *testing.T) {
	leased := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{
				JobIdStr:   jobIdString,
				ExecutorId: executorId,
				PodRequirementsOverlay: &schedulerobjects.PodRequirements{
					Tolerations: []v1.Toleration{
						{
							Key:    "whale",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
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
					Created:   protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobIdStr: jobIdString,
				RunIdStr: runIdString,
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
					Created:  protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobIdStr: jobIdString,
				RunIdStr: runIdString,
				Errors: []*armadaevents.Error{
					{
						Terminal: false,
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
					Created:      protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobIdStr: jobIdString,
				RunIdStr: runIdString,
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
					Created:      protoutil.ToTimestamp(baseTime),
					RunAttempted: true,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(leaseReturned))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobError(t *testing.T) {
	errored := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobErrors{
			JobErrors: &armadaevents.JobErrors{
				JobIdStr: jobIdString,
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
								Message:          "The pod was terminated",
								NodeName:         nodeName,
								PodNumber:        podNumber,
								KubernetesReason: armadaevents.KubernetesReason_DeadlineExceeded,
								ContainerErrors: []*armadaevents.ContainerError{
									{
										ObjectMeta: &armadaevents.ObjectMeta{
											Name: "container1",
										},
										ExitCode:         -1,
										Message:          "container1 Error",
										Reason:           "container1 Reason",
										KubernetesReason: armadaevents.KubernetesReason_OOM,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	maxRunsExceeded := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobErrors{
			JobErrors: &armadaevents.JobErrors{
				JobIdStr: jobIdString,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_MaxRunsExceeded{
							MaxRunsExceeded: &armadaevents.MaxRunsExceeded{
								Message: "Max runs",
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
					Created:      protoutil.ToTimestamp(baseTime),
					Cause:        api.Cause_DeadlineExceeded,
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
		{
			Events: &api.EventMessage_Failed{
				Failed: &api.JobFailedEvent{
					JobId:    jobIdString,
					Reason:   "Max runs",
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  protoutil.ToTimestamp(baseTime),
					Cause:    api.Cause_Error,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(errored, maxRunsExceeded))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobSucceeded(t *testing.T) {
	succeeded := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobSucceeded{
			JobSucceeded: &armadaevents.JobSucceeded{
				JobIdStr: jobIdString,
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
					Created:      protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{
				RunIdStr: runIdString,
				JobIdStr: jobIdString,
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
					Created:      protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobIdStr: jobIdString,
				RunIdStr: runIdString,
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
		Created: baseTimeProto,
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
					Created:  protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunAssigned{
			JobRunAssigned: &armadaevents.JobRunAssigned{
				RunIdStr: runIdString,
				JobIdStr: jobIdString,
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
					Created:      protoutil.ToTimestamp(baseTime),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_ResourceUtilisation{
			ResourceUtilisation: &armadaevents.ResourceUtilisation{
				RunIdStr: runIdString,
				JobIdStr: jobIdString,
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
				MaxResourcesForPeriod: map[string]*resource.Quantity{
					"cpu": resourcePointer("2.0"),
					"mem": resourcePointer("100Gi"),
				},
				TotalCumulativeUsage: map[string]*resource.Quantity{
					"cpu": resourcePointer("3.0"),
					"mem": resourcePointer("200Gi"),
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
					Created:      protoutil.ToTimestamp(baseTime),
					ClusterId:    executorId,
					KubernetesId: runIdString,
					MaxResourcesForPeriod: map[string]*resource.Quantity{
						"cpu": resourcePointer("2.0"),
						"mem": resourcePointer("100Gi"),
					},
					NodeName:     nodeName,
					PodNumber:    podNumber,
					PodName:      podName,
					PodNamespace: namespace,
					TotalCumulativeUsage: map[string]*resource.Quantity{
						"cpu": resourcePointer("3.0"),
						"mem": resourcePointer("200Gi"),
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
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_StandaloneIngressInfo{
			StandaloneIngressInfo: &armadaevents.StandaloneIngressInfo{
				RunIdStr: runIdString,
				JobIdStr: jobIdString,
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
					Created:      protoutil.ToTimestamp(baseTime),
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

func TestConvertJobPreemptionRequested(t *testing.T) {
	preemptRequest := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobPreemptionRequested{
			JobPreemptionRequested: &armadaevents.JobPreemptionRequested{
				JobIdStr: jobIdString,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Preempting{
				Preempting: &api.JobPreemptingEvent{
					JobId:     jobIdString,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   protoutil.ToTimestamp(baseTime),
					Requestor: userId,
				},
			},
		},
	}

	apiEvents, err := FromEventSequence(toEventSeq(preemptRequest))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func TestConvertJobRunPreempted(t *testing.T) {
	preempted := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{
				PreemptedJobIdStr: jobIdString,
				PreemptedRunIdStr: runIdString,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Preempted{
				Preempted: &api.JobPreemptedEvent{
					JobId:    jobIdString,
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  protoutil.ToTimestamp(baseTime),
					RunId:    runIdString,
				},
			},
		},
	}

	// both PreemptiveJobId and PreemptiveRunId not nil
	apiEvents, err := FromEventSequence(toEventSeq(preempted))
	assert.NoError(t, err)
	assert.Equal(t, expected, apiEvents)
}

func toEventSeq(event ...*armadaevents.EventSequence_Event) *armadaevents.EventSequence {
	return &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events:     event,
		UserId:     userId,
	}
}

func resourcePointer(s string) *resource.Quantity {
	r := resource.MustParse(s)
	return &r
}

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
	jobId      = "01f3j0g1md4qx7z5qb148qnh4r"
	runId      = "123e4567-e89b-12d3-a456-426614174000"
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

var (
	baseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	baseTimeProto = protoutil.ToTimestamp(baseTime)
)

func TestConvertSubmitted(t *testing.T) {
	// Submit
	submit := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:    jobId,
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
					JobId:    jobId,
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  protoutil.ToTimestamp(baseTime),
					Job: &api.Job{
						Id:         jobId,
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
				JobId:    jobId,
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
				JobId: jobId,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Cancelling{
				Cancelling: &api.JobCancellingEvent{
					JobId:     jobId,
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
				JobId: jobId,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Cancelled{
				Cancelled: &api.JobCancelledEvent{
					JobId:     jobId,
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
				JobId: jobId,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Reprioritizing{
				Reprioritizing: &api.JobReprioritizingEvent{
					JobId:     jobId,
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
				JobId: jobId,
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Reprioritized{
				Reprioritized: &api.JobReprioritizedEvent{
					JobId:     jobId,
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
				JobId:      jobId,
				ExecutorId: executorId,
				PodRequirementsOverlay: &schedulerobjects.PodRequirements{
					Tolerations: []*v1.Toleration{
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
					JobId:     jobId,
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
				JobId: jobId,
				RunId: runId,
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
					JobId:    jobId,
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
				JobId: jobId,
				RunId: runId,
				Errors: []*armadaevents.Error{
					{
						Terminal: false,
						Reason: &armadaevents.Error_PodUnschedulable{
							PodUnschedulable: &armadaevents.PodUnschedulable{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runId,
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
					JobId:        jobId,
					ClusterId:    executorId,
					PodNamespace: namespace,
					PodName:      podName,
					KubernetesId: runId,
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
				JobId: jobId,
				RunId: runId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodLeaseReturned{
							PodLeaseReturned: &armadaevents.PodLeaseReturned{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runId,
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
					JobId:        jobId,
					ClusterId:    executorId,
					KubernetesId: runId,
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
				JobId: jobId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   executorId,
									Namespace:    namespace,
									Name:         podName,
									KubernetesId: runId,
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
				JobId: jobId,
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
					JobId:        jobId,
					ClusterId:    executorId,
					PodNamespace: namespace,
					PodName:      podName,
					NodeName:     nodeName,
					KubernetesId: runId,
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
					JobId:    jobId,
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
				JobId: jobId,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   executorId,
							Namespace:    namespace,
							Name:         podName,
							KubernetesId: runId,
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
					JobId:        jobId,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      protoutil.ToTimestamp(baseTime),
					ClusterId:    executorId,
					KubernetesId: runId,
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
				RunId: runId,
				JobId: jobId,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   executorId,
							Namespace:    namespace,
							Name:         podName,
							KubernetesId: runId,
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
					JobId:        jobId,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      protoutil.ToTimestamp(baseTime),
					ClusterId:    executorId,
					KubernetesId: runId,
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
				JobId: jobId,
				RunId: runId,
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
					JobId:    jobId,
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
				RunId: runId,
				JobId: jobId,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   executorId,
							Namespace:    namespace,
							Name:         podName,
							KubernetesId: runId,
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
					JobId:        jobId,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      protoutil.ToTimestamp(baseTime),
					ClusterId:    executorId,
					KubernetesId: runId,
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
				RunId: runId,
				JobId: jobId,
				ResourceInfo: &armadaevents.KubernetesResourceInfo{
					ObjectMeta: &armadaevents.ObjectMeta{
						ExecutorId:   executorId,
						Namespace:    namespace,
						Name:         podName,
						KubernetesId: runId,
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
				AvgResourcesForPeriod: map[string]*resource.Quantity{
					"cpu": resourcePointer("2.5"),
					"mem": resourcePointer("150Gi"),
				},
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Utilisation{
				Utilisation: &api.JobUtilisationEvent{
					JobId:        jobId,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      protoutil.ToTimestamp(baseTime),
					ClusterId:    executorId,
					KubernetesId: runId,
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
					AvgResourcesForPeriod: map[string]*resource.Quantity{
						"cpu": resourcePointer("2.5"),
						"mem": resourcePointer("150Gi"),
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
				RunId: runId,
				JobId: jobId,
				ObjectMeta: &armadaevents.ObjectMeta{
					ExecutorId:   executorId,
					Namespace:    namespace,
					Name:         podName,
					KubernetesId: runId,
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
					JobId:        jobId,
					JobSetId:     jobSetName,
					Queue:        queue,
					Created:      protoutil.ToTimestamp(baseTime),
					ClusterId:    executorId,
					KubernetesId: runId,
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
				JobId:  jobId,
				Reason: "preemption requested",
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Preempting{
				Preempting: &api.JobPreemptingEvent{
					JobId:     jobId,
					JobSetId:  jobSetName,
					Queue:     queue,
					Created:   protoutil.ToTimestamp(baseTime),
					Requestor: userId,
					Reason:    "preemption requested",
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
				PreemptedJobId: jobId,
				PreemptedRunId: runId,
				Reason:         "Preempted reason",
			},
		},
	}

	expected := []*api.EventMessage{
		{
			Events: &api.EventMessage_Preempted{
				Preempted: &api.JobPreemptedEvent{
					JobId:    jobId,
					JobSetId: jobSetName,
					Queue:    queue,
					Created:  protoutil.ToTimestamp(baseTime),
					RunId:    runId,
					Reason:   "Preempted reason",
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

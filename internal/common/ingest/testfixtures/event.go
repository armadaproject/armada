package testfixtures

import (
	"time"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

// Standard Set of events for common tests
const (
	JobId                      = "01f3j0g1md4qx7z5qb148qnh4r"
	RunId                      = "123e4567-e89b-12d3-a456-426614174000"
	PartitionMarkerGroupId     = "223e4567-e89b-12d3-a456-426614174000"
	JobsetName                 = "testJobset"
	ExecutorId                 = "testCluster"
	ExecutorId2                = "testCluster2"
	ExecutorId3                = "testCluster3"
	CancelUser                 = "canceluser"
	NodeName                   = "testNode"
	Pool                       = "Pool"
	PodName                    = "test-pod"
	Queue                      = "test-Queue"
	UserId                     = "testUser"
	Namespace                  = "test-ns"
	Priority                   = 3
	NewPriority                = 4
	PodNumber                  = 6
	ExitCode                   = 322
	ErrMsg                     = "sample error message"
	DebugMsg                   = "sample debug message"
	LeaseReturnedMsg           = "lease returned error message"
	UnschedulableMsg           = "test pod is unschedulable"
	PreemptionReason           = "job preempted"
	PartitionMarkerPartitionId = 456

	ExecutorCordonReason = "bad executor"
)

var (
	PartitionMarkerGroupIdUuid = uuid.MustParse(PartitionMarkerGroupId)
	PriorityClassName          = "test-priority"
	Groups                     = []string{"group1", "group2"}
	NodeSelector               = map[string]string{"foo": "bar"}
	Affinity                   = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "noode-name",
								Operator: v1.NodeSelectorOpNotIn,
								Values:   []string{"node-1"},
							},
						},
					},
				},
			},
		},
	}
	Tolerations = []v1.Toleration{{
		Key:      "fish",
		Operator: "exists",
	}}
	BaseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	BaseTimeProto = protoutil.ToTimestamp(BaseTime)
)

var ScheduledAtPriority = int32(15)

var Submit = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_SubmitJob{
		SubmitJob: &armadaevents.SubmitJob{
			JobId:           JobId,
			Priority:        Priority,
			AtMostOnce:      true,
			Preemptible:     true,
			ConcurrencySafe: true,
			ObjectMeta: &armadaevents.ObjectMeta{
				Namespace: Namespace,
				Name:      "test-job",
			},
			MainObject: &armadaevents.KubernetesMainObject{
				ObjectMeta: &armadaevents.ObjectMeta{
					Annotations: map[string]string{
						"foo":                            "bar",
						configuration.FailFastAnnotation: "true",
					},
				},
				Object: &armadaevents.KubernetesMainObject_PodSpec{
					PodSpec: &armadaevents.PodSpecWithAvoidList{
						PodSpec: &v1.PodSpec{
							NodeSelector:      NodeSelector,
							Tolerations:       Tolerations,
							PriorityClassName: PriorityClassName,
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

var Assigned = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunAssigned{
		JobRunAssigned: &armadaevents.JobRunAssigned{
			RunId: RunId,
			JobId: JobId,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						KubernetesId: RunId,
						Name:         PodName,
						Namespace:    Namespace,
						ExecutorId:   ExecutorId,
					},
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							PodNumber: PodNumber,
						},
					},
				},
			},
		},
	},
}

var Leased = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunLeased{
		JobRunLeased: &armadaevents.JobRunLeased{
			RunId:                  RunId,
			JobId:                  JobId,
			ExecutorId:             ExecutorId,
			NodeId:                 NodeName,
			Pool:                   Pool,
			HasScheduledAtPriority: true,
			ScheduledAtPriority:    15,
			UpdateSequenceNumber:   1,
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

var Running = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunRunning{
		JobRunRunning: &armadaevents.JobRunRunning{
			RunId: RunId,
			JobId: JobId,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							NodeName:  NodeName,
							PodNumber: PodNumber,
						},
					},
				},
			},
		},
	},
}

var JobRunSucceeded = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: RunId,
			JobId: JobId,
		},
	},
}

var JobRunCancelled = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunCancelled{
		JobRunCancelled: &armadaevents.JobRunCancelled{
			RunId: RunId,
			JobId: JobId,
		},
	},
}

var LeaseReturned = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobId,
			RunId: RunId,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodLeaseReturned{
						PodLeaseReturned: &armadaevents.PodLeaseReturned{
							Message:      LeaseReturnedMsg,
							RunAttempted: true,
						},
					},
				},
			},
		},
	},
}

var JobCancelRequested = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_CancelJob{
		CancelJob: &armadaevents.CancelJob{
			JobId: JobId,
		},
	},
}

var JobSetCancelRequested = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_CancelJobSet{
		CancelJobSet: &armadaevents.CancelJobSet{},
	},
}

var JobCancelled = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId:      JobId,
			CancelUser: CancelUser,
		},
	},
}

var JobValidated = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobValidated{
		JobValidated: &armadaevents.JobValidated{
			JobId: JobId,
			Pools: []string{"cpu"},
		},
	},
}

var JobRequeued = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRequeued{
		JobRequeued: &armadaevents.JobRequeued{
			JobId: JobId,
			SchedulingInfo: &schedulerobjects.JobSchedulingInfo{
				Lifetime:        0,
				AtMostOnce:      true,
				Preemptible:     true,
				ConcurrencySafe: true,
				Version:         0,
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								NodeSelector: NodeSelector,
								Tolerations: armadaslices.Map(Tolerations, func(t v1.Toleration) *v1.Toleration {
									return &t
								}),
								PreemptionPolicy: "PreemptLowerPriority",
								Affinity:         Affinity,
								ResourceRequirements: &v1.ResourceRequirements{
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
			UpdateSequenceNumber: 2,
		},
	},
}

var PartitionMarker = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_PartitionMarker{
		PartitionMarker: &armadaevents.PartitionMarker{
			GroupId:   PartitionMarkerGroupId,
			Partition: PartitionMarkerPartitionId,
		},
	},
}

var JobReprioritiseRequested = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
		ReprioritiseJob: &armadaevents.ReprioritiseJob{
			JobId:    JobId,
			Priority: NewPriority,
		},
	},
}

var JobSetReprioritiseRequested = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_ReprioritiseJobSet{
		ReprioritiseJobSet: &armadaevents.ReprioritiseJobSet{
			Priority: NewPriority,
		},
	},
}

var JobReprioritised = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
		ReprioritisedJob: &armadaevents.ReprioritisedJob{
			JobId:    JobId,
			Priority: NewPriority,
		},
	},
}

var JobPreemptionRequested = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobPreemptionRequested{
		JobPreemptionRequested: &armadaevents.JobPreemptionRequested{
			JobId: JobId,
		},
	},
}

var JobRunPreempted = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunPreempted{
		JobRunPreempted: &armadaevents.JobRunPreempted{
			PreemptedJobId: JobId,
			PreemptedRunId: RunId,
			Reason:         PreemptionReason,
		},
	},
}

var JobRunFailed = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobId,
			RunId: RunId,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodError{
						PodError: &armadaevents.PodError{
							Message:      ErrMsg,
							DebugMessage: DebugMsg,
							NodeName:     NodeName,
							ContainerErrors: []*armadaevents.ContainerError{
								{ExitCode: ExitCode},
							},
						},
					},
				},
			},
		},
	},
}

var JobRunUnschedulable = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobId,
			RunId: RunId,
			Errors: []*armadaevents.Error{
				{
					Terminal: false,
					Reason: &armadaevents.Error_PodUnschedulable{
						PodUnschedulable: &armadaevents.PodUnschedulable{
							NodeName: NodeName,
							ObjectMeta: &armadaevents.ObjectMeta{
								ExecutorId: ExecutorId,
							},
							Message: UnschedulableMsg,
						},
					},
				},
			},
		},
	},
}

var JobPreempted = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobErrors{
		JobErrors: &armadaevents.JobErrors{
			JobId: JobId,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_JobRunPreemptedError{
						JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
					},
				},
			},
		},
	},
}

var JobRejected = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobErrors{
		JobErrors: &armadaevents.JobErrors{
			JobId: JobId,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_JobRejected{
						JobRejected: &armadaevents.JobRejected{
							Message: ErrMsg,
						},
					},
				},
			},
		},
	},
}

var JobFailed = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobErrors{
		JobErrors: &armadaevents.JobErrors{
			JobId: JobId,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodError{
						PodError: &armadaevents.PodError{
							Message:  ErrMsg,
							NodeName: NodeName,
							ContainerErrors: []*armadaevents.ContainerError{
								{ExitCode: ExitCode},
							},
						},
					},
				},
			},
		},
	},
}

var JobSucceeded = &armadaevents.EventSequence_Event{
	Created: testfixtures.BasetimeProto,
	Event: &armadaevents.EventSequence_Event_JobSucceeded{
		JobSucceeded: &armadaevents.JobSucceeded{
			JobId: JobId,
		},
	},
}

var UpsertExecutorSettingsCordon = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_ExecutorSettingsUpsert{
		ExecutorSettingsUpsert: &controlplaneevents.ExecutorSettingsUpsert{
			Name:         ExecutorId,
			Cordoned:     true,
			CordonReason: ExecutorCordonReason,
		},
	},
}

var UpsertExecutorSettingsUncordon = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_ExecutorSettingsUpsert{
		ExecutorSettingsUpsert: &controlplaneevents.ExecutorSettingsUpsert{
			Name:         ExecutorId,
			Cordoned:     false,
			CordonReason: "",
		},
	},
}

var DeleteExecutorSettings = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_ExecutorSettingsDelete{
		ExecutorSettingsDelete: &controlplaneevents.ExecutorSettingsDelete{
			Name: ExecutorId,
		},
	},
}

var PreemptOnExecutor = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_PreemptOnExecutor{
		PreemptOnExecutor: &controlplaneevents.PreemptOnExecutor{
			Name:            ExecutorId,
			Queues:          []string{Queue},
			PriorityClasses: []string{PriorityClassName},
		},
	},
}

var CancelOnExecutor = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_CancelOnExecutor{
		CancelOnExecutor: &controlplaneevents.CancelOnExecutor{
			Name:            ExecutorId,
			Queues:          []string{Queue},
			PriorityClasses: []string{PriorityClassName},
		},
	},
}

var PreemptOnQueue = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_PreemptOnQueue{
		PreemptOnQueue: &controlplaneevents.PreemptOnQueue{
			Name:            Queue,
			PriorityClasses: []string{PriorityClassName},
		},
	},
}

var CancelQueuedOnQueue = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_CancelOnQueue{
		CancelOnQueue: &controlplaneevents.CancelOnQueue{
			Name:            Queue,
			PriorityClasses: []string{PriorityClassName},
			JobStates:       []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_QUEUED},
		},
	},
}

var CancelRunningOnQueue = &controlplaneevents.Event{
	Event: &controlplaneevents.Event_CancelOnQueue{
		CancelOnQueue: &controlplaneevents.CancelOnQueue{
			Name:            Queue,
			PriorityClasses: []string{PriorityClassName},
			JobStates:       []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_RUNNING},
		},
	},
}

func JobSetCancelRequestedWithStateFilter(states ...armadaevents.JobState) *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Created: testfixtures.BasetimeProto,
		Event: &armadaevents.EventSequence_Event_CancelJobSet{
			CancelJobSet: &armadaevents.CancelJobSet{
				States: states,
			},
		},
	}
}

func DeepCopy(events *armadaevents.EventSequence_Event) (*armadaevents.EventSequence_Event, error) {
	bytes, err := proto.Marshal(events)
	if err != nil {
		return nil, err
	}
	var copied armadaevents.EventSequence_Event
	err = proto.Unmarshal(bytes, &copied)
	if err != nil {
		return nil, err
	}
	return &copied, nil
}

func NewEventSequence(event ...*armadaevents.EventSequence_Event) *armadaevents.EventSequence {
	return &armadaevents.EventSequence{
		Queue:      Queue,
		JobSetName: JobsetName,
		Events:     event,
		UserId:     UserId,
		Groups:     Groups,
	}
}

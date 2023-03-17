package testfixtures

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Standard Set of events for common tests
const (
	JobIdString                  = "01f3j0g1md4qx7z5qb148qnh4r"
	RunIdString                  = "123e4567-e89b-12d3-a456-426614174000"
	PartitionMarkerGroupIdString = "223e4567-e89b-12d3-a456-426614174000"
)

var (
	JobIdProto, _               = armadaevents.ProtoUuidFromUlidString(JobIdString)
	RunIdProto                  = armadaevents.ProtoUuidFromUuid(uuid.MustParse(RunIdString))
	PartitionMarkerGroupIdProto = armadaevents.ProtoUuidFromUuid(uuid.MustParse(PartitionMarkerGroupIdString))
	JobIdUuid                   = armadaevents.UuidFromProtoUuid(JobIdProto)
	RunIdUuid                   = armadaevents.UuidFromProtoUuid(RunIdProto)
	PartitionMarkerGroupIdUuid  = armadaevents.UuidFromProtoUuid(PartitionMarkerGroupIdProto)
	PriorityClassName           = "test-priority"
	PriorityClassValue          = int32(100)
	PriorityClasses             = map[string]configuration.PriorityClass{PriorityClassName: {Priority: PriorityClassValue}}
	Groups                      = []string{"group1", "group2"}
	NodeSelector                = map[string]string{"foo": "bar"}
	Tolerations                 = []v1.Toleration{{
		Key:      "fish",
		Operator: "exists",
	}}
	BaseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
)

const (
	JobSetName                 = "testJobset"
	ExecutorId                 = "testCluster"
	NodeName                   = "testNode"
	PodName                    = "test-pod"
	Queue                      = "test-Queue"
	UserId                     = "testUser"
	Namespace                  = "test-ns"
	Priority                   = 3
	NewPriority                = 4
	PodNumber                  = 6
	ExitCode                   = 322
	ErrMsg                     = "sample error message"
	LeaseReturnedMsg           = "lease returned error message"
	TerminatedMsg              = "test pod terminated message"
	UnschedulableMsg           = "test pod is unschedulable"
	PartitionMarkerPartitionId = 456
)

var Submit = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_SubmitJob{
		SubmitJob: &armadaevents.SubmitJob{
			JobId:           JobIdProto,
			Priority:        Priority,
			AtMostOnce:      true,
			Preemptible:     true,
			ConcurrencySafe: true,
			ObjectMeta: &armadaevents.ObjectMeta{
				Namespace: Namespace,
				Name:      "test-job",
			},
			MainObject: &armadaevents.KubernetesMainObject{
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
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunAssigned{
		JobRunAssigned: &armadaevents.JobRunAssigned{
			RunId: RunIdProto,
			JobId: JobIdProto,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						KubernetesId: RunIdString,
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
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunLeased{
		JobRunLeased: &armadaevents.JobRunLeased{
			RunId:      RunIdProto,
			JobId:      JobIdProto,
			ExecutorId: ExecutorId,
			NodeId:     NodeName,
		},
	},
}

var Running = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunRunning{
		JobRunRunning: &armadaevents.JobRunRunning{
			RunId: RunIdProto,
			JobId: JobIdProto,
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
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: RunIdProto,
			JobId: JobIdProto,
		},
	},
}

var LeaseReturned = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobIdProto,
			RunId: RunIdProto,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodLeaseReturned{
						PodLeaseReturned: &armadaevents.PodLeaseReturned{
							Message: LeaseReturnedMsg,
						},
					},
				},
			},
		},
	},
}

var JobCancelRequested = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_CancelJob{
		CancelJob: &armadaevents.CancelJob{
			JobId: JobIdProto,
		},
	},
}

var JobSetCancelRequested = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_CancelJobSet{
		CancelJobSet: &armadaevents.CancelJobSet{},
	},
}

var JobCancelled = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId: JobIdProto,
		},
	},
}

var PartitionMarker = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_PartitionMarker{
		PartitionMarker: &armadaevents.PartitionMarker{
			GroupId:   PartitionMarkerGroupIdProto,
			Partition: PartitionMarkerPartitionId,
		},
	},
}

var JobReprioritiseRequested = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
		ReprioritiseJob: &armadaevents.ReprioritiseJob{
			JobId:    JobIdProto,
			Priority: NewPriority,
		},
	},
}

var JobSetReprioritiseRequested = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_ReprioritiseJobSet{
		ReprioritiseJobSet: &armadaevents.ReprioritiseJobSet{
			Priority: NewPriority,
		},
	},
}

var JobReprioritised = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
		ReprioritisedJob: &armadaevents.ReprioritisedJob{
			JobId:    JobIdProto,
			Priority: NewPriority,
		},
	},
}

var JobPreempted = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunPreempted{
		JobRunPreempted: &armadaevents.JobRunPreempted{
			PreemptedJobId: JobIdProto,
			PreemptedRunId: RunIdProto,
		},
	},
}

var JobRunFailed = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobIdProto,
			RunId: RunIdProto,
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

var JobRunTerminated = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobIdProto,
			RunId: RunIdProto,
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodTerminated{
						PodTerminated: &armadaevents.PodTerminated{
							NodeName: NodeName,
							ObjectMeta: &armadaevents.ObjectMeta{
								ExecutorId: ExecutorId,
							},
							Message: TerminatedMsg,
						},
					},
				},
			},
		},
	},
}

var JobRunUnschedulable = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobIdProto,
			RunId: RunIdProto,
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

var JobFailed = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobErrors{
		JobErrors: &armadaevents.JobErrors{
			JobId: JobIdProto,
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

var JobLeaseReturned = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobRunErrors{
		JobRunErrors: &armadaevents.JobRunErrors{
			JobId: JobIdProto,
			RunId: eventutil.LegacyJobRunId(),
			Errors: []*armadaevents.Error{
				{
					Terminal: true,
					Reason: &armadaevents.Error_PodLeaseReturned{
						PodLeaseReturned: &armadaevents.PodLeaseReturned{
							ObjectMeta: &armadaevents.ObjectMeta{
								ExecutorId: ExecutorId,
							},
							Message: LeaseReturnedMsg,
						},
					},
				},
			},
		},
	},
}

var JobSucceeded = &armadaevents.EventSequence_Event{
	Created: &BaseTime,
	Event: &armadaevents.EventSequence_Event_JobSucceeded{
		JobSucceeded: &armadaevents.JobSucceeded{
			JobId: JobIdProto,
		},
	},
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
		JobSetName: JobSetName,
		Events:     event,
		UserId:     UserId,
		Groups:     Groups,
	}
}

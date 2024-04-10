package testfixtures

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

var (
	DefaultNamespace     = "testNamespace"
	DefaultOwner         = "testUser"
	DefaultJobset        = "testJobset"
	DefaultQueue         = queue.Queue{Name: "testQueue"}
	DefaultPrincipal     = authorization.NewStaticPrincipal(DefaultOwner, []string{"groupA"})
	DefaultContainerPort = v1.ContainerPort{
		Name:          "testContainerPort",
		HostPort:      8080,
		ContainerPort: 8080,
		Protocol:      "TCP",
	}
	DefaultResources = v1.ResourceRequirements{
		Requests: v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
		Limits: v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
	}
	DefaultContainers = []v1.Container{
		{
			Name:      "testContainer",
			Ports:     []v1.ContainerPort{DefaultContainerPort},
			Resources: DefaultResources,
		},
	}
	DefaultTolerations = []v1.Toleration{
		{
			Key:      "armadaproject.io/foo",
			Operator: "Exists",
		},
	}
	DefaultPriorityInt                   = uint32(1000)
	DefaultPriorityFloat                 = float64(DefaultPriorityInt)
	DefaultPriorityClass                 = "testPriorityClass"
	DefaultTerminationGracePeriodSeconds = int64(30)
	DefaultActiveDeadlineSeconds         = int64(3600)
	DefaultTime                          = time.Now().UTC()
	DefaultHostNameSuffix                = "testHostNameSuffix"
	DefaultCertNameSuffix                = "testHostNameSuffix"
)

func DefaultSubmissionConfig() configuration.SubmissionConfig {
	return configuration.SubmissionConfig{
		AllowedPriorityClassNames: map[string]bool{DefaultPriorityClass: true},
		DefaultPriorityClassName:  DefaultPriorityClass,
		DefaultJobLimits:          armadaresource.ComputeResources{"cpu": resource.MustParse("1")},
		DefaultJobTolerations:     DefaultTolerations,
		MaxPodSpecSizeBytes:       1000,
		MinJobResources:           map[v1.ResourceName]resource.Quantity{},
		MinTerminationGracePeriod: 30 * time.Second,
		MaxTerminationGracePeriod: 300 * time.Second,
		DefaultActiveDeadline:     1 * time.Hour,
		IngressConfig: configuration.IngressConfiguration{
			HostnameSuffix: DefaultHostNameSuffix,
			CertNameSuffix: DefaultCertNameSuffix,
		},
	}
}

func NEventSequenceEvents(n int) []*armadaevents.EventSequence_Event {
	events := make([]*armadaevents.EventSequence_Event, n)
	for i := 0; i < n; i++ {
		events[i] = &armadaevents.EventSequence_Event{
			Created: &DefaultTime,
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: SubmitJob(i + 1),
			},
		}
	}
	return events
}

func SubmitRequestWithNItems(n int) *api.JobSubmitRequest {
	items := make([]*api.JobSubmitRequestItem, n)
	for i := 0; i < n; i++ {
		items[i] = JobSubmitRequestItem(i + 1)
	}
	return &api.JobSubmitRequest{
		Queue:           "testQueue",
		JobSetId:        "testJobset",
		JobRequestItems: items,
	}
}

func JobSubmitRequestItem(i int) *api.JobSubmitRequestItem {
	return &api.JobSubmitRequestItem{
		Priority:  DefaultPriorityFloat,
		Namespace: DefaultNamespace,
		ClientId:  fmt.Sprintf("%d", i),
		PodSpec: &v1.PodSpec{
			TerminationGracePeriodSeconds: pointer.Int64(DefaultTerminationGracePeriodSeconds),
			ActiveDeadlineSeconds:         pointer.Int64(DefaultActiveDeadlineSeconds),
			PriorityClassName:             DefaultPriorityClass,
			Containers:                    DefaultContainers,
		},
	}
}

func SubmitJob(i int) *armadaevents.SubmitJob {
	return &armadaevents.SubmitJob{
		JobId:           TestUlid(i),
		Priority:        DefaultPriorityInt,
		ObjectMeta:      &armadaevents.ObjectMeta{Namespace: DefaultNamespace},
		Objects:         []*armadaevents.KubernetesObject{},
		DeduplicationId: fmt.Sprintf("%d", i),
		MainObject: &armadaevents.KubernetesMainObject{
			Object: &armadaevents.KubernetesMainObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: &v1.PodSpec{
						TerminationGracePeriodSeconds: pointer.Int64(DefaultTerminationGracePeriodSeconds),
						ActiveDeadlineSeconds:         pointer.Int64(DefaultActiveDeadlineSeconds),
						PriorityClassName:             DefaultPriorityClass,
						Tolerations:                   DefaultTolerations,
						Containers:                    DefaultContainers,
					},
				},
			},
		},
	}
}

// TestUlidGenerator returns a function that Generates ulids starting at "00000000000000000000000001" and
// incrementing by one each time it is called
func TestUlidGenerator() func() *armadaevents.Uuid {
	counter := 0
	return func() *armadaevents.Uuid {
		counter++
		return TestUlid(counter)
	}
}

func TestUlid(i int) *armadaevents.Uuid {
	ulid := fmt.Sprintf("000000000000000000000000%02X", i)
	return armadaevents.MustProtoUuidFromUlidString(ulid)
}

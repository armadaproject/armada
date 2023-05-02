package utilisation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	fakeContext "github.com/armadaproject/armada/internal/executor/fake/context"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/pkg/api"
)

var testPodResources = domain.UtilisationData{
	CurrentUsage: armadaresource.ComputeResources{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("640Ki"),
	},
	CumulativeUsage: armadaresource.ComputeResources{
		"cpu": resource.MustParse("10"),
	},
}

func TestUtilisationEventReporter_ReportUtilisationEvents(t *testing.T) {
	reportingPeriod := 100 * time.Millisecond
	clusterContext := fakeContext.NewFakeClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}, "kubernetes.io/hostname", nil)
	fakeEventReporter := &mocks.FakeEventReporter{}
	fakeUtilisationService := &fakePodUtilisationService{data: &testPodResources}

	eventReporter := NewUtilisationEventReporter(clusterContext, fakeUtilisationService, fakeEventReporter, reportingPeriod, true)
	_, err := submitPod(clusterContext)
	require.NoError(t, err)

	deadline := time.Now().Add(time.Second)
	for {
		eventReporter.ReportUtilisationEvents()
		time.Sleep(time.Millisecond)

		if len(fakeEventReporter.ReceivedEvents) >= 2 || time.Now().After(deadline) {
			break
		}
	}

	assert.True(t, len(fakeEventReporter.ReceivedEvents) >= 2)
	event1 := fakeEventReporter.ReceivedEvents[0].Event.(*api.JobUtilisationEvent)
	event2 := fakeEventReporter.ReceivedEvents[1].Event.(*api.JobUtilisationEvent)

	assert.Equal(t, testPodResources.CurrentUsage, armadaresource.ComputeResources(event1.MaxResourcesForPeriod))
	assert.Equal(t, testPodResources.CumulativeUsage, armadaresource.ComputeResources(event1.TotalCumulativeUsage))

	period := event2.Created.Sub(event1.Created)

	accuracy := time.Millisecond * 20
	assert.Equal(t, period/accuracy, reportingPeriod/accuracy)
}

func TestUtilisationEventReporter_ReportUtilisationEvents_WhenNoUtilisationData(t *testing.T) {
	reportingPeriod := 100 * time.Millisecond
	clusterContext := fakeContext.NewFakeClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}, "kubernetes.io/hostname", nil)
	fakeEventReporter := &mocks.FakeEventReporter{}
	fakeUtilisationService := &fakePodUtilisationService{data: domain.EmptyUtilisationData()}

	eventReporter := NewUtilisationEventReporter(clusterContext, fakeUtilisationService, fakeEventReporter, reportingPeriod, true)
	_, err := submitPod(clusterContext)
	require.NoError(t, err)

	deadline := time.Now().Add(time.Millisecond * 500)
	count := 0
	for {
		eventReporter.ReportUtilisationEvents()
		count++
		time.Sleep(time.Millisecond)
		if time.Now().After(deadline) {
			break
		}
	}

	assert.True(t, len(fakeEventReporter.ReceivedEvents) == 0)
	assert.True(t, count > 0)
}

func submitPod(clusterContext context.ClusterContext) (*v1.Pod, error) {
	podResources := map[v1.ResourceName]resource.Quantity{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("640Ki"),
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			Labels: map[string]string{
				domain.JobId: "test-job",
			},
		},
		Spec: v1.PodSpec{Containers: []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Limits:   podResources,
					Requests: podResources,
				},
			},
		}},
	}

	return clusterContext.SubmitPod(pod, "owner", nil)
}

type fakePodUtilisationService struct {
	data *domain.UtilisationData
}

func (f *fakePodUtilisationService) GetPodUtilisation(pod *v1.Pod) *domain.UtilisationData {
	return f.data.DeepCopy()
}

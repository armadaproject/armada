package utilisation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/domain"
	fakeContext "github.com/G-Research/armada/internal/executor/fake/context"
	reporter_fake "github.com/G-Research/armada/internal/executor/reporter/fake"
	"github.com/G-Research/armada/pkg/api"
)

var testPodResources = common.ComputeResources{
	"cpu":    resource.MustParse("1"),
	"memory": resource.MustParse("640Ki"),
}

func TestUtilisationEventReporter_ReportUtilisationEvents(t *testing.T) {
	reportingPeriod := 100 * time.Millisecond
	clusterContext := fakeContext.NewFakeClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}, nil)
	fakeEventReporter := &reporter_fake.FakeEventReporter{}
	reporter := NewUtilisationEventReporter(clusterContext, &fakePodUtilisation{}, fakeEventReporter, reportingPeriod)

	podResources := map[v1.ResourceName]resource.Quantity{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("640Ki"),
	}

	clusterContext.SubmitPod(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			Labels: map[string]string{
				domain.JobId: "test-job",
			},
		},
		Spec: v1.PodSpec{Containers: []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					podResources, podResources,
				},
			},
		}}}, "owner", nil)

	deadline := time.Now().Add(time.Second)
	for {
		reporter.ReportUtilisationEvents()
		time.Sleep(time.Millisecond)

		if len(fakeEventReporter.ReceivedEvents) >= 2 || time.Now().After(deadline) {
			break
		}
	}

	assert.True(t, len(fakeEventReporter.ReceivedEvents) >= 2)
	event1 := fakeEventReporter.ReceivedEvents[0].(*api.JobUtilisationEvent)
	event2 := fakeEventReporter.ReceivedEvents[1].(*api.JobUtilisationEvent)

	assert.Equal(t, testPodResources, common.ComputeResources(event1.MaxResourcesForPeriod))

	period := event2.Created.Sub(event1.Created)

	accuracy := time.Millisecond * 10
	assert.Equal(t, period/accuracy, reportingPeriod/accuracy)
}

type fakePodUtilisation struct{}

func (f *fakePodUtilisation) GetPodUtilisation(pod *v1.Pod) common.ComputeResources {
	return testPodResources
}

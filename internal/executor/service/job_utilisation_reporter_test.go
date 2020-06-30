package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/domain"
	fakeContext "github.com/G-Research/armada/internal/executor/fake/context"
	"github.com/G-Research/armada/pkg/api"
)

var testPodResources = common.ComputeResources{
	"cpu":    resource.MustParse("1"),
	"memory": resource.MustParse("640Ki"),
}

func TestUtilisationEventReporter_ReportUtilisationEvents(t *testing.T) {
	reportingPeriod := 100 * time.Millisecond
	clusterContext := fakeContext.NewFakeClusterContext("test")
	fakeEventReporter := &fakeEventReporter{}
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
		}}}, "owner")

	deadline := time.Now().Add(time.Second)
	for {
		reporter.ReportUtilisationEvents()
		time.Sleep(time.Millisecond)

		if len(fakeEventReporter.receivedEvents) >= 2 || time.Now().After(deadline) {
			break
		}
	}

	assert.True(t, len(fakeEventReporter.receivedEvents) >= 2)
	event1 := fakeEventReporter.receivedEvents[0].(*api.JobUtilisationEvent)
	event2 := fakeEventReporter.receivedEvents[1].(*api.JobUtilisationEvent)

	assert.Equal(t, testPodResources, common.ComputeResources(event1.MaxResourcesForPeriod))

	period := event2.Created.Sub(event1.Created)

	accuracy := time.Millisecond * 10
	assert.Equal(t, period/accuracy, reportingPeriod/accuracy)
}

type fakePodUtilisation struct{}

func (f *fakePodUtilisation) GetPodUtilisation(pod *v1.Pod) common.ComputeResources {
	return testPodResources
}

type fakeEventReporter struct {
	receivedEvents []api.Event
}

func (f *fakeEventReporter) Report(event api.Event) error {
	f.receivedEvents = append(f.receivedEvents, event)
	return nil
}

func (f *fakeEventReporter) QueueEvent(event api.Event, callback func(error)) {
	e := f.Report(event)
	callback(e)
}

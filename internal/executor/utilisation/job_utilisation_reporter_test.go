package utilisation

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	fakeContext "github.com/armadaproject/armada/internal/executor/fake/context"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var testPodResources = domain.UtilisationData{
	CurrentUsage: armadaresource.ComputeResources{
		"cpu":    resource.MustParse("1000m"),
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

	eventReporter, err := NewUtilisationEventReporter(clusterContext, fakeUtilisationService, fakeEventReporter, reportingPeriod)
	require.NoError(t, err)
	_, err = submitPod(clusterContext)
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
	assert.Len(t, fakeEventReporter.ReceivedEvents[0].Event.Events, 1)
	event1, ok := fakeEventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_ResourceUtilisation)
	assert.True(t, ok)
	assert.Len(t, fakeEventReporter.ReceivedEvents[1].Event.Events, 1)
	_, ok = fakeEventReporter.ReceivedEvents[1].Event.Events[0].Event.(*armadaevents.EventSequence_Event_ResourceUtilisation)
	assert.True(t, ok)

	assert.Equal(t, testPodResources.CurrentUsage, armadaresource.FromProtoMap(event1.ResourceUtilisation.MaxResourcesForPeriod))
	assert.Equal(t, testPodResources.CurrentUsage, armadaresource.FromProtoMap(event1.ResourceUtilisation.AvgResourcesForPeriod))
	assert.Equal(t, testPodResources.CumulativeUsage, armadaresource.FromProtoMap(event1.ResourceUtilisation.TotalCumulativeUsage))

	event1CreatedTime := fakeEventReporter.ReceivedEvents[0].Event.Events[0].Created
	event2CreatedTime := fakeEventReporter.ReceivedEvents[1].Event.Events[0].Created
	period := protoutil.ToStdTime(event2CreatedTime).Sub(protoutil.ToStdTime(event1CreatedTime))

	accuracy := time.Millisecond * 20
	assert.Equal(t, period/accuracy, reportingPeriod/accuracy)
}

func TestUtilisationEventReporter_ReportUtilisationEvents_WhenNoUtilisationData(t *testing.T) {
	reportingPeriod := 100 * time.Millisecond
	clusterContext := fakeContext.NewFakeClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}, "kubernetes.io/hostname", nil)
	fakeEventReporter := &mocks.FakeEventReporter{}
	fakeUtilisationService := &fakePodUtilisationService{data: domain.EmptyUtilisationData()}

	eventReporter, err := NewUtilisationEventReporter(clusterContext, fakeUtilisationService, fakeEventReporter, reportingPeriod)
	require.NoError(t, err)
	_, err = submitPod(clusterContext)
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
				domain.JobId:    util2.NewULID(),
				domain.JobRunId: uuid.New().String(),
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

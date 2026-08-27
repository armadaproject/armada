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

	// Reporting is normally driven by a task scheduler, so the poll below stands
	// in for the ticks while waiting for two reporting periods to elapse
	require.Eventually(t, func() bool {
		eventReporter.ReportUtilisationEvents()
		return len(fakeEventReporter.GetReceivedEvents()) >= 2
	}, time.Second, time.Millisecond, "two utilisation events must be reported")

	receivedEvents := fakeEventReporter.GetReceivedEvents()
	require.GreaterOrEqual(t, len(receivedEvents), 2)
	assert.Len(t, receivedEvents[0].Event.Events, 1)
	event1, ok := receivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_ResourceUtilisation)
	assert.True(t, ok)
	assert.Len(t, receivedEvents[1].Event.Events, 1)
	_, ok = receivedEvents[1].Event.Events[0].Event.(*armadaevents.EventSequence_Event_ResourceUtilisation)
	assert.True(t, ok)

	assert.Equal(t, testPodResources.CurrentUsage, armadaresource.FromProtoMap(event1.ResourceUtilisation.MaxResourcesForPeriod))
	assert.Equal(t, testPodResources.CurrentUsage, armadaresource.FromProtoMap(event1.ResourceUtilisation.AvgResourcesForPeriod))
	assert.Equal(t, testPodResources.CumulativeUsage, armadaresource.FromProtoMap(event1.ResourceUtilisation.TotalCumulativeUsage))

	event1CreatedTime := receivedEvents[0].Event.Events[0].Created
	event2CreatedTime := receivedEvents[1].Event.Events[0].Created
	period := protoutil.ToStdTime(event2CreatedTime).Sub(protoutil.ToStdTime(event1CreatedTime))

	// The reporter must hold the second event back for a full reporting period.
	// The lower bound is what the reporter guarantees. The upper bound only
	// checks the event was period-driven rather than immediate, with generous
	// slack because a busy runner can delay the polling loop past the period.
	assert.GreaterOrEqual(t, period, reportingPeriod-2*time.Millisecond)
	assert.Less(t, period, reportingPeriod+100*time.Millisecond)
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

	assert.Never(t, func() bool {
		eventReporter.ReportUtilisationEvents()
		return len(fakeEventReporter.GetReceivedEvents()) > 0
	}, 500*time.Millisecond, time.Millisecond, "empty utilisation data must not produce events")
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

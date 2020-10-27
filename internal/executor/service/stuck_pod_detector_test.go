package service

import (
	"testing"
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/pkg/api"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestStuckPodDetector_DoesNothingIfNoPodsAreFound(t *testing.T) {
	_, mockLeaseService, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	stuckPodDetector.HandleStuckPods()

	assert.Zero(t, mockLeaseService.returnLeaseCalls)

	mockLeaseService.assertReportDoneCalledOnceWith(t, emptyPodSlice())
}

func TestStuckPodDetector_DoesNothingIfNoStuckPodsAreFound(t *testing.T) {
	runningPod := makeRunningPod()

	fakeClusterContext, mockLeaseService, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, runningPod)

	stuckPodDetector.HandleStuckPods()

	assert.Zero(t, mockLeaseService.returnLeaseCalls)

	mockLeaseService.assertReportDoneCalledOnceWith(t, emptyPodSlice())
}

func TestStuckPodDetector_DeletesPodAndReportsDoneIfStuckAndUnretryable(t *testing.T) {
	unretryableStuckPod := makeUnretryableStuckPod()

	fakeClusterContext, mockLeaseService, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, unretryableStuckPod)

	stuckPodDetector.HandleStuckPods()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, emptyPodSlice(), remainingActivePods)

	assert.Zero(t, mockLeaseService.returnLeaseCalls)

	mockLeaseService.assertReportDoneCalledOnceWith(t, []*v1.Pod{unretryableStuckPod})
}

func TestStuckPodDetector_ReturnsLeaseAndDeletesRetryableStuckPod(t *testing.T) {
	retryableStuckPod := makeRetryableStuckPod()

	fakeClusterContext, mockLeaseService, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, retryableStuckPod)

	stuckPodDetector.HandleStuckPods()

	// Not done as can be retried
	assert.Equal(t, 1, mockLeaseService.reportDoneCalls)
	assert.Equal(t, emptyPodSlice(), mockLeaseService.reportDoneArg)

	// Not returning lease yet
	assert.Equal(t, 0, mockLeaseService.returnLeaseCalls)

	// Still deletes pod
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, emptyPodSlice(), remainingActivePods)

	stuckPodDetector.HandleStuckPods()

	// Not done as can be retried
	assert.Equal(t, 2, mockLeaseService.reportDoneCalls)
	assert.Equal(t, emptyPodSlice(), mockLeaseService.reportDoneArg)

	// Return lease for retry
	assert.Equal(t, 1, mockLeaseService.returnLeaseCalls)
	assert.Equal(t, retryableStuckPod, mockLeaseService.returnLeaseArg)
}

func getActivePods(t *testing.T, clusterContext context.ClusterContext) []*v1.Pod {
	t.Helper()
	remainingActivePods, err := clusterContext.GetActiveBatchPods()
	if err != nil {
		t.Error(err)
	}
	return remainingActivePods
}

func makeRunningPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{Phase: "Running"})
}

func makeUnretryableStuckPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{
		Phase: "Pending",
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "ImagePullBackOff",
						Message: "Some message",
					},
				},
			},
		},
	})
}

func makeRetryableStuckPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{
		Phase: "Pending",
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "Some reason",
						Message: "Some message",
					},
				},
			},
		},
	})
}

func makeTestPod(status v1.PodStatus) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobId: "job-id-1",
				domain.Queue: "queue-id-1",
			},
			Annotations: map[string]string{
				domain.JobSetId: "job-set-id-1",
			},
			CreationTimestamp: metav1.Time{time.Now().Add(-10 * time.Minute)},
		},
		Status: status,
	}
}

func addPod(t *testing.T, fakeClusterContext context.ClusterContext, runningPod *v1.Pod) {
	t.Helper()
	_, err := fakeClusterContext.SubmitPod(runningPod, "owner-1")
	if err != nil {
		t.Error(err)
	}
}

func makeStuckPodDetectorWithTestDoubles() (context.ClusterContext, *mockLeaseService, *StuckPodDetector) {
	fakeClusterContext := newSyncFakeClusterContext()
	mockLeaseService := NewMockLeaseService()

	stuckPodDetector := NewPodProgressMonitorService(
		fakeClusterContext,
		&FakeEventReporter{nil},
		mockLeaseService,
		time.Second)

	return fakeClusterContext, mockLeaseService, stuckPodDetector
}

func emptyPodSlice() []*v1.Pod {
	return make([]*v1.Pod, 0)
}

type mockLeaseService struct {
	returnLeaseCalls      int
	requestJobLeasesCalls int
	reportDoneCalls       int

	returnLeaseArg *v1.Pod
	reportDoneArg  []*v1.Pod
}

func NewMockLeaseService() *mockLeaseService {
	return &mockLeaseService{0, 0, 0, nil, nil}
}

func (ls *mockLeaseService) ReturnLease(pod *v1.Pod) error {
	ls.returnLeaseArg = pod
	ls.returnLeaseCalls++
	return nil
}

func (ls *mockLeaseService) RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error) {
	ls.requestJobLeasesCalls++
	return make([]*api.Job, 0), nil
}

func (ls *mockLeaseService) ReportDone(pods []*v1.Pod) error {
	ls.reportDoneArg = pods
	ls.reportDoneCalls++
	return nil
}

func (ls *mockLeaseService) assertReportDoneCalledOnceWith(t *testing.T, expected []*v1.Pod) {
	assert.Equal(t, 1, ls.reportDoneCalls)
	assert.Equal(t, expected, ls.reportDoneArg)
}

type syncFakeClusterContext struct {
	pods map[string]*v1.Pod
}

func newSyncFakeClusterContext() *syncFakeClusterContext {
	c := &syncFakeClusterContext{pods: map[string]*v1.Pod{}}
	return c
}

func (*syncFakeClusterContext) Stop() {}

func (c *syncFakeClusterContext) AddPodEventHandler(handler cache.ResourceEventHandlerFuncs) {}

func (c *syncFakeClusterContext) GetBatchPods() ([]*v1.Pod, error) {
	pods := make([]*v1.Pod, 0, len(c.pods))
	for _, p := range c.pods {
		pods = append(pods, p.DeepCopy())
	}
	return pods, nil
}

func (c *syncFakeClusterContext) GetAllPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *syncFakeClusterContext) GetActiveBatchPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *syncFakeClusterContext) GetNodes() ([]*v1.Node, error) {
	return make([]*v1.Node, 0), nil
}

func (c *syncFakeClusterContext) GetPodEvents(pod *v1.Pod) ([]*v1.Event, error) {
	return []*v1.Event{}, nil
}

func (c *syncFakeClusterContext) SubmitPod(pod *v1.Pod, owner string) (*v1.Pod, error) {
	c.pods[pod.Labels[domain.JobId]] = pod
	return pod, nil
}

func (c *syncFakeClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
	return nil
}

func (c *syncFakeClusterContext) DeletePods(pods []*v1.Pod) {
	for _, p := range pods {
		delete(c.pods, p.Labels[domain.JobId])
	}
}

func (c *syncFakeClusterContext) GetClusterId() string {
	return "cluster-id-1"
}

package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/job"

	reporter_fake "github.com/G-Research/armada/internal/executor/reporter/fake"
	"github.com/G-Research/armada/pkg/api"
)

func TestStuckPodDetector_DoesNothingIfNoPodsAreFound(t *testing.T) {
	_, mockLeaseService, _, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	stuckPodDetector.HandleStuckPods()

	assert.Zero(t, mockLeaseService.returnLeaseCalls)

	mockLeaseService.assertReportDoneCalledOnceWith(t, []string{})
}

func TestStuckPodDetector_DoesNothingIfNoStuckPodsAreFound(t *testing.T) {
	runningPod := makeRunningPod()

	fakeClusterContext, mockLeaseService, _, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, runningPod)

	stuckPodDetector.HandleStuckPods()

	assert.Zero(t, mockLeaseService.returnLeaseCalls)

	mockLeaseService.assertReportDoneCalledOnceWith(t, []string{})
}

func TestStuckPodDetector_DeletesPodAndReportsDoneIfStuckAndUnretryable(t *testing.T) {
	unretryableStuckPod := makeUnretryableStuckPod()

	fakeClusterContext, mockLeaseService, eventsReporter, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, unretryableStuckPod)

	stuckPodDetector.HandleStuckPods()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Zero(t, mockLeaseService.returnLeaseCalls)

	mockLeaseService.assertReportDoneCalledOnceWith(t, []string{unretryableStuckPod.Labels[domain.JobId]})

	_, ok := eventsReporter.ReceivedEvents[0].(*api.JobUnableToScheduleEvent)
	assert.True(t, ok)

	stuckPodDetector.HandleStuckPods()

	failedEvent, ok := eventsReporter.ReceivedEvents[1].(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Contains(t, failedEvent.Reason, "unrecoverable problem")
}

func TestStuckPodDetector_DeletesPodAndReportsFailedIfStuckTerminating(t *testing.T) {
	terminatingPod := makeTerminatingPod()

	fakeClusterContext, mockLeaseService, eventsReporter, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, terminatingPod)

	stuckPodDetector.HandleStuckPods()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Zero(t, mockLeaseService.returnLeaseCalls)
	mockLeaseService.assertReportDoneCalledOnceWith(t, []string{terminatingPod.Labels[domain.JobId]})

	stuckPodDetector.HandleStuckPods()

	failedEvent, ok := eventsReporter.ReceivedEvents[0].(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Contains(t, failedEvent.Reason, "terminating")
}

func TestStuckPodDetector_ReturnsLeaseAndDeletesRetryableStuckPod(t *testing.T) {
	retryableStuckPod := makeRetryableStuckPod()

	fakeClusterContext, mockLeaseService, _, stuckPodDetector := makeStuckPodDetectorWithTestDoubles()

	addPod(t, fakeClusterContext, retryableStuckPod)

	stuckPodDetector.HandleStuckPods()

	// Not done as can be retried
	assert.Equal(t, 1, mockLeaseService.reportDoneCalls)
	assert.Equal(t, []string{}, mockLeaseService.reportDoneArg)

	// Not returning lease yet
	assert.Equal(t, 0, mockLeaseService.returnLeaseCalls)

	// Still deletes pod
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	stuckPodDetector.HandleStuckPods()

	// Not done as can be retried
	assert.Equal(t, 2, mockLeaseService.reportDoneCalls)
	assert.Equal(t, []string{}, mockLeaseService.reportDoneArg)

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

func makeTerminatingPod() *v1.Pod {
	pod := makeTestPod(v1.PodStatus{Phase: "Running"})
	t := metav1.NewTime(time.Now().Add(-time.Hour))
	pod.DeletionTimestamp = &t
	return pod
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
			UID:               types.UID(util.NewULID()),
		},
		Status: status,
	}
}

func addPod(t *testing.T, fakeClusterContext context.ClusterContext, runningPod *v1.Pod) {
	t.Helper()
	_, err := fakeClusterContext.SubmitPod(runningPod, "owner-1", []string{})
	if err != nil {
		t.Error(err)
	}
}

func makeStuckPodDetectorWithTestDoubles() (context.ClusterContext, *mockLeaseService, *reporter_fake.FakeEventReporter, *StuckPodDetector) {
	fakeClusterContext := newSyncFakeClusterContext()
	jobContext := job.NewClusterJobContext(fakeClusterContext)
	mockLeaseService := NewMockLeaseService()
	eventReporter := &reporter_fake.FakeEventReporter{nil}

	stuckPodDetector := NewPodProgressMonitorService(
		fakeClusterContext,
		jobContext,
		eventReporter,
		mockLeaseService,
		time.Second)

	return fakeClusterContext, mockLeaseService, eventReporter, stuckPodDetector
}

type mockLeaseService struct {
	returnLeaseCalls      int
	requestJobLeasesCalls int
	reportDoneCalls       int

	returnLeaseArg *v1.Pod
	reportDoneArg  []string
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

func (ls *mockLeaseService) ReportDone(jobIds []string) error {
	ls.reportDoneArg = jobIds
	ls.reportDoneCalls++
	return nil
}

func (ls *mockLeaseService) assertReportDoneCalledOnceWith(t *testing.T, expected []string) {
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

func (c *syncFakeClusterContext) SubmitService(service *v1.Service) (*v1.Service, error) {
	return nil, fmt.Errorf("Services not implemented in syncFakeClusterContext")
}

func (c *syncFakeClusterContext) GetService(name string, namespace string) (*v1.Service, error) {
	return nil, fmt.Errorf("Services not implemented in syncFakeClusterContext")
}

func (c *syncFakeClusterContext) DeleteService(service *v1.Service) error {
	return fmt.Errorf("Services not implemented in syncFakeClusterContext")
}

func (c *syncFakeClusterContext) SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error) {
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

func (c *syncFakeClusterContext) GetClusterPool() string {
	return "pool"
}

func (c *syncFakeClusterContext) GetNodeStatsSummary(node *v1.Node) (*v1alpha1.Summary, error) {
	return &v1alpha1.Summary{}, nil
}

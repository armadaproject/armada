package service

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	context2 "github.com/G-Research/armada/internal/executor/fake/context"
	"github.com/G-Research/armada/pkg/api"
)

func TestCanBeRemovedConditions(t *testing.T) {
	s := CreateLeaseService(time.Second, time.Second)
	pods := map[*v1.Pod]bool{
		// should not be cleaned yet
		makePodWithCurrentStateReported(v1.PodRunning, false):   false,
		makePodWithCurrentStateReported(v1.PodSucceeded, false): false,
		makePodWithCurrentStateReported(v1.PodFailed, false):    false,

		// should be cleaned
		makePodWithCurrentStateReported(v1.PodSucceeded, true): true,
		makePodWithCurrentStateReported(v1.PodFailed, true):    true,
	}

	for pod, expected := range pods {
		result := s.canBeRemoved(pod)
		assert.Equal(t, expected, result)
	}
}

func TestCanBeRemovedMinumumPodTime(t *testing.T) {
	s := CreateLeaseService(5*time.Minute, 10*time.Minute)
	now := time.Now()
	pods := map[*v1.Pod]bool{
		// should not be cleaned yet
		makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-1*time.Minute)): false,
		makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-1*time.Minute)):    false,
		makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-7*time.Minute)):    false,

		// should be cleaned
		makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-7*time.Minute)): true,
		makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-13*time.Minute)):   true,
	}

	for pod, expected := range pods {
		result := s.canBeRemoved(pod)
		assert.Equal(t, expected, result)
	}
}

func TestChunkPods(t *testing.T) {
	p := makePodWithCurrentStateReported(v1.PodPending, false)
	chunks := chunkPods([]*v1.Pod{p, p, p}, 2)
	assert.Equal(t, [][]*v1.Pod{{p, p}, {p}}, chunks)
}

func makeFinishedPodWithTimestamp(state v1.PodPhase, timestamp time.Time) *v1.Pod {
	pod := makePodWithCurrentStateReported(state, true)
	pod.CreationTimestamp.Time = timestamp
	return pod
}

func makePodWithCurrentStateReported(state v1.PodPhase, reportedDone bool) *v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{string(state): time.Now().String()},
			// all times check fallback to creation time if there is no info available
			CreationTimestamp: metav1.Time{time.Now().Add(-10 * time.Minute)},
		},
		Status: v1.PodStatus{
			Phase: state,
		},
	}

	if reportedDone {
		pod.Annotations[jobDoneAnnotation] = time.Now().String()
	}

	return &pod
}

func CreateLeaseService(minimumPodAge, failedPodExpiry time.Duration) *JobLeaseService {
	fakeClusterContext := context2.NewFakeClusterContext("test")
	return NewJobLeaseService(fakeClusterContext, &queueClientMock{}, minimumPodAge, failedPodExpiry)
}

type queueClientMock struct {
}

func (queueClientMock) LeaseJobs(ctx context.Context, in *api.LeaseRequest, opts ...grpc.CallOption) (*api.JobLease, error) {
	return &api.JobLease{}, nil
}

func (queueClientMock) RenewLease(ctx context.Context, in *api.RenewLeaseRequest, opts ...grpc.CallOption) (*api.IdList, error) {
	return &api.IdList{}, nil
}

func (queueClientMock) ReturnLease(ctx context.Context, in *api.ReturnLeaseRequest, opts ...grpc.CallOption) (*types.Empty, error) {
	return &types.Empty{}, nil
}

func (queueClientMock) ReportDone(ctx context.Context, in *api.IdList, opts ...grpc.CallOption) (*api.IdList, error) {
	return &api.IdList{}, nil
}

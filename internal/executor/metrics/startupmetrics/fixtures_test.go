package startupmetrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/constants"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// baseTime is the instant every fixture is relative to. A function rather than a package variable,
// so each caller builds its own value and nothing is shared between tests.
func baseTime() time.Time {
	return time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
}

// baselineWithOffset is a point on the fixture timeline: baseTime plus a number of seconds.
func baselineWithOffset(offsetSeconds int) *types.Timestamp {
	return protoutil.ToTimestamp(baseTime().Add(time.Duration(offsetSeconds) * time.Second))
}

func seconds(count int) *types.Duration {
	return protoutil.ToDuration(time.Duration(count) * time.Second)
}

func sizeBytes(size uint64) *types.UInt64Value {
	return &types.UInt64Value{Value: size}
}

func testPod(queue, pool, node string) *v1.Pod {
	pod := podWithSpec(1, 1)
	pod.Labels = map[string]string{domain.Queue: queue}
	pod.Annotations = map[string]string{constants.PoolAnnotation: pool}
	pod.Spec.NodeName = node
	return pod
}

func podWithSpec(initContainers int, containers int) *v1.Pod {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}}
	for i := 0; i < initContainers; i++ {
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, v1.Container{Name: fmt.Sprintf("init-%d", i)})
	}
	for i := 0; i < containers; i++ {
		pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{Name: fmt.Sprintf("main-%d", i)})
	}
	return pod
}

// fullyMeasuredInfo is a complete breakdown, with nothing missing: created at +0s, scheduled at
// +2s, sandbox ready at +9s, one init container pulling its image and running for 41s, and the app
// container started at +137s from an image already on the node.
func fullyMeasuredInfo() *armadaevents.PodStartUpInfo {
	return &armadaevents.PodStartUpInfo{
		CreatedAt:             baselineWithOffset(0),
		ScheduledAt:           baselineWithOffset(2),
		KubeletAcknowledgedAt: baselineWithOffset(3),
		SandboxReadyAt:        baselineWithOffset(9),
		StartedAt:             baselineWithOffset(137),
		InitContainers: []*armadaevents.ContainerStartUpInfo{
			{
				Name:       "init",
				CreatedAt:  baselineWithOffset(69),
				StartedAt:  baselineWithOffset(70),
				FinishedAt: baselineWithOffset(111),
				ImagePull: &armadaevents.ImagePullInfo{
					Duration:    seconds(58),
					WaitingTime: seconds(3),
					SizeBytes:   sizeBytes(2415919104),
				},
			},
		},
		Containers: []*armadaevents.ContainerStartUpInfo{
			{
				Name:      "main",
				CreatedAt: baselineWithOffset(135),
				StartedAt: baselineWithOffset(137),
				// An image already on the node: the kubelet's message carries no duration or
				// waiting time, and the size is resolved separately, best effort.
				ImagePull: &armadaevents.ImagePullInfo{Cached: true, SizeBytes: sizeBytes(734003200)},
			},
		},
	}
}

// observations reads one series of a histogram. The series is created if it does not exist, so a
// metric that was never observed reads as a count of zero rather than failing the lookup.
func observations(t *testing.T, vec *prometheus.HistogramVec, labelValues ...string) (uint64, float64) {
	t.Helper()
	observer, err := vec.GetMetricWithLabelValues(labelValues...)
	require.NoError(t, err)

	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum()
}

func observationCount(t *testing.T, vec *prometheus.HistogramVec, labelValues ...string) uint64 {
	t.Helper()
	count, _ := observations(t, vec, labelValues...)
	return count
}

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/armadaproject/armada/internal/podwriter"
)

func TestLatencyStats(t *testing.T) {
	latencies := make([]time.Duration, 100)
	for i := range latencies {
		latencies[i] = time.Duration(i+1) * time.Millisecond
	}
	mean, p50, p90, p99, maxMs := latencyStats(latencies)
	assert.InDelta(t, 50.5, mean, 0.01)
	assert.InDelta(t, 50, p50, 0.01)
	assert.InDelta(t, 90, p90, 0.01)
	assert.InDelta(t, 99, p99, 0.01)
	assert.InDelta(t, 100, maxMs, 0.01)

	mean, p50, p90, p99, maxMs = latencyStats(nil)
	assert.Zero(t, mean+p50+p90+p99+maxMs)
}

func TestMakePodAnnotationPadding(t *testing.T) {
	restore := snapshotFlags()
	defer restore()

	*namePrefix = "bench"
	*namespace = "ns"
	*nodeName = ""

	*podAnnotationsBytes = 0
	assert.Nil(t, makePod(3).Annotations)

	*podAnnotationsBytes = 1024
	pod := makePod(3)
	require.Len(t, pod.Annotations, 1)
	assert.Len(t, pod.Annotations["bench/padding"], 1024)
	assert.Equal(t, "bench-00003", pod.Name)
}

func TestSeriesSum(t *testing.T) {
	text := `# HELP apiserver_request_total counter
apiserver_request_total{code="201",component="apiserver",dry_run="",group="",resource="pods",scope="resource",subresource="",verb="POST",version="v1"} 100
apiserver_request_total{code="201",component="apiserver",dry_run="",group="",resource="pods",scope="resource",subresource="",verb="POST",version="v1"} 50
apiserver_request_total{code="429",component="apiserver",dry_run="",group="",resource="pods",scope="resource",subresource="",verb="POST",version="v1"} 3
apiserver_request_total{code="201",component="apiserver",dry_run="",group="",resource="pods",scope="resource",subresource="log",verb="POST",version="v1"} 9
apiserver_request_total{code="500",component="apiserver",dry_run="",group="",resource="pods",scope="resource",subresource="",verb="POST",version="v1"} 2
apiserver_current_inflight_requests{request_kind="mutating"} 12
apiserver_current_inflight_requests{request_kind="read"} 4
process_resident_memory_bytes 1024
`
	postPods := []string{`verb="POST"`, `resource="pods"`, `group=""`, `version="v1"`, `subresource=""`}
	assert.InDelta(t, 155, seriesSum(text, "apiserver_request_total", postPods...), 0.001)
	assert.InDelta(t, 150, seriesSum(text, "apiserver_request_total", append(postPods, `code="201"`)...), 0.001)
	assert.InDelta(t, 3, seriesSum(text, "apiserver_request_total", append(postPods, `code="429"`)...), 0.001)
	assert.InDelta(t, 2, seriesSum(text, "apiserver_request_total", append(postPods, `code="5`)...), 0.001)
	assert.InDelta(t, 12, seriesSum(text, "apiserver_current_inflight_requests", `request_kind="mutating"`), 0.001)
	assert.InDelta(t, 1024, seriesSum(text, "process_resident_memory_bytes"), 0.001)
	assert.Zero(t, seriesSum(text, "etcd_request_duration_seconds", `operation="create"`))

	// Kubernetes also exposes the etcd metric with resource= instead of type=.
	modernEtcd := `etcd_request_duration_seconds_sum{group="",operation="create",resource="pods"} 1.5
etcd_request_duration_seconds_count{group="",operation="create",resource="pods"} 1
apiserver_resource_objects{group="",resource="pods"} 7
`
	assert.Zero(t, seriesSum(modernEtcd, "etcd_request_duration_seconds_sum", `type="pods"`))
	assert.InDelta(t, 1.5, seriesSum(modernEtcd, "etcd_request_duration_seconds_sum", `resource="pods"`), 0.001)
	assert.Equal(t, "apiserver_resource_objects", firstMetric(modernEtcd, "apiserver_resource_objects", "apiserver_storage_objects"))
	assert.Equal(t, "apiserver_resource_objects", firstMetric(modernEtcd, "apiserver_storage_objects", "apiserver_resource_objects"))
}

func TestDeltaAndMean(t *testing.T) {
	before := `apiserver_request_duration_seconds_sum{verb="POST",resource="pods"} 10
apiserver_request_duration_seconds_count{verb="POST",resource="pods"} 5
etcd_request_duration_seconds_sum{operation="create",type="pods"} 2
etcd_request_duration_seconds_count{operation="create",type="pods"} 1
`
	after := `apiserver_request_duration_seconds_sum{verb="POST",resource="pods"} 30
apiserver_request_duration_seconds_count{verb="POST",resource="pods"} 15
etcd_request_duration_seconds_sum{operation="create",type="pods"} 6
etcd_request_duration_seconds_count{operation="create",type="pods"} 1
`
	labels := []string{`verb="POST"`, `resource="pods"`}
	assert.InDelta(t, 20, delta(before, after, "apiserver_request_duration_seconds_sum", labels...), 0.001)
	// 20s added over 10 requests -> 2s mean (Stop converts to ms).
	assert.InDelta(t, 2, meanOfDelta(before, after,
		"apiserver_request_duration_seconds_sum", "apiserver_request_duration_seconds_count", labels...), 0.001)
	// 0 new samples must not divide by zero.
	assert.Zero(t, meanOfDelta(before, after,
		"etcd_request_duration_seconds_sum", "etcd_request_duration_seconds_count",
		`operation="create"`, `type="pods"`))
}

// The api path must stream pods to workers; a 20k run of large specs cannot
// hold every pod object at once.
func TestRunAPICreatesAllPods(t *testing.T) {
	restore := snapshotFlags()
	defer restore()

	*namePrefix = "bench"
	*namespace = "ns"
	*nodeName = "bench-nonexistent"
	*concurrency = 8
	*rateLimit = 0
	*podAnnotationsBytes = 0

	client := k8sfake.NewSimpleClientset()
	results, latencies := runAPI(context.Background(), client, 100)

	require.Len(t, results, 100)
	require.Len(t, latencies, 100)
	created := 0
	for _, result := range results {
		if result.Status == podwriter.Created {
			created++
			assert.NotZero(t, result.Name)
		}
	}
	assert.Equal(t, 100, created)

	pods, err := client.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, pods.Items, 100)
}

func TestRunAPIPacedCompletes(t *testing.T) {
	restore := snapshotFlags()
	defer restore()

	*namePrefix = "paced"
	*namespace = "ns"
	*nodeName = ""
	*concurrency = 4
	*rateLimit = 500 // 20 pods should take ~40ms, allowing generous slack
	*podAnnotationsBytes = 0

	client := k8sfake.NewSimpleClientset()
	start := time.Now()
	results, _ := runAPI(context.Background(), client, 20)
	elapsed := time.Since(start)

	created := 0
	for _, result := range results {
		if result.Status == podwriter.Created {
			created++
		}
	}
	assert.Equal(t, 20, created)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
}

func TestRunDeleteRemovesPodsAndIsIdempotent(t *testing.T) {
	restore := snapshotFlags()
	defer restore()

	*namePrefix = "del"
	*namespace = "ns"
	*concurrency = 8
	*rateLimit = 0
	*podAnnotationsBytes = 0

	client := k8sfake.NewSimpleClientset()
	_, _ = runAPI(context.Background(), client, 50)

	results, latencies := runDelete(context.Background(), client, 60) // 10 never existed
	assert.Len(t, latencies, 60)
	for _, result := range results {
		assert.Equal(t, podwriter.Created, result.Status, result.Reason)
	}

	pods, err := client.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, pods.Items)
}

func snapshotFlags() func() {
	savedConcurrency, savedRateLimit, savedAnnotations := *concurrency, *rateLimit, *podAnnotationsBytes
	savedNamePrefix, savedNamespace, savedNodeName := *namePrefix, *namespace, *nodeName
	return func() {
		*concurrency, *rateLimit, *podAnnotationsBytes = savedConcurrency, savedRateLimit, savedAnnotations
		*namePrefix, *namespace, *nodeName = savedNamePrefix, savedNamespace, savedNodeName
	}
}

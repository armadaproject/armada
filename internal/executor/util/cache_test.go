package util

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
)

func initializeTest() {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
}

func TestMapPodCache_Add(t *testing.T) {
	initializeTest()

	pod := makeManagedPod("job1")
	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")
	cache.Add(pod)

	assert.Equal(t, pod, cache.Get(ExtractPodKey(pod)))
	assert.Equal(t, 1, getMetricGaugeCurrentValue(cache))
}

func TestMapPodCache_Add_Metrics(t *testing.T) {
	initializeTest()

	pod := makeManagedPod("job1")
	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")

	// Repeated add to the same key, only counts as 1
	cache.Add(pod)
	cache.Add(pod)
	assert.Equal(t, 1, getMetricGaugeCurrentValue(cache))
}

func TestMapPodCache_Add_Expires(t *testing.T) {
	initializeTest()

	pod := makeManagedPod("job1")
	cache := NewTimeExpiringPodCache(time.Second/10, time.Second/100, "metric1")
	cache.Add(pod)

	assert.Equal(t, pod, cache.Get(ExtractPodKey(pod)))

	time.Sleep(time.Second / 5)

	assert.Equal(t, (*v1.Pod)(nil), cache.Get("job1"))
	assert.Equal(t, 0, len(cache.GetAll()))
	assert.Equal(t, 0, getMetricGaugeCurrentValue(cache))
}

func TestMapPodCache_AddIfNotExists(t *testing.T) {
	initializeTest()

	pod1 := makeManagedPod("job1")
	pod1.Name = "1"
	pod2 := makeManagedPod("job1")
	pod2.Name = "2"

	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")
	assert.True(t, cache.AddIfNotExists(pod1))
	assert.False(t, cache.AddIfNotExists(pod2))
	assert.Equal(t, "1", cache.Get(ExtractPodKey(pod1)).Name)
	assert.Equal(t, 1, getMetricGaugeCurrentValue(cache))
}

func TestMapPodCache_Update(t *testing.T) {
	initializeTest()

	pod1 := makeManagedPod("job1")
	pod1.Name = "1"
	pod2 := makeManagedPod("job1")
	pod2.Name = "2"

	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")
	assert.False(t, cache.Update(ExtractPodKey(pod1), pod1))
	assert.Equal(t, 0, len(cache.GetAll()))
	assert.Equal(t, 0, getMetricGaugeCurrentValue(cache))
	cache.Add(pod1)
	assert.True(t, cache.Update(ExtractPodKey(pod2), pod2))
	assert.Equal(t, "2", cache.Get(ExtractPodKey(pod1)).Name)
	assert.Equal(t, 1, getMetricGaugeCurrentValue(cache))
}

func TestMapPodCache_Delete(t *testing.T) {
	initializeTest()

	pod := makeManagedPod("job1")
	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")

	cache.Add(pod)
	assert.NotNil(t, cache.Get(ExtractPodKey(pod)))
	assert.Equal(t, 1, getMetricGaugeCurrentValue(cache))

	cache.Delete(ExtractPodKey(pod))
	assert.Nil(t, cache.Get(ExtractPodKey(pod)))
	assert.Equal(t, 0, getMetricGaugeCurrentValue(cache))
}

func TestMapPodCache_Delete_DoNotFailOnUnrecognisedKey(t *testing.T) {
	initializeTest()

	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")

	cache.Delete("madeupkey")
	assert.Nil(t, cache.Get("madeupkey"))
}

func TestNewMapPodCache_Get_ReturnsCopy(t *testing.T) {
	initializeTest()

	pod := makeManagedPod("job1")
	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")

	cache.Add(pod)

	result := cache.Get(ExtractPodKey(pod))
	assert.Equal(t, result, pod)

	pod.Namespace = "new value"
	assert.NotEqual(t, result, pod)
}

func TestMapPodCache_GetAll(t *testing.T) {
	initializeTest()

	pod1 := makeManagedPod("job1")
	pod2 := makeManagedPod("job2")
	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")

	cache.Add(pod1)
	cache.Add(pod2)

	result := cache.GetAll()

	assert.Equal(t, len(result), 2)

	if ExtractJobId(result[0]) == "job1" {
		assert.True(t, ExtractJobId(result[0]) == "job1")
		assert.True(t, ExtractJobId(result[1]) == "job2")
	} else {
		assert.True(t, ExtractJobId(result[0]) == "job2")
		assert.True(t, ExtractJobId(result[1]) == "job1")
	}
}

func TestMapPodCache_GetReturnsACopy(t *testing.T) {
	initializeTest()

	pod := makeManagedPod("job1")
	cache := NewTimeExpiringPodCache(time.Minute, time.Second, "metric1")

	cache.Add(pod)

	result := cache.GetAll()[0]
	assert.Equal(t, result, pod)

	pod.Namespace = "new value"
	assert.NotEqual(t, result, pod)
}

func makeManagedPod(jobId string) *v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobId:     jobId,
				domain.PodNumber: "0",
			},
		},
	}
	return &pod
}

func getMetricGaugeCurrentValue(cache *mapPodCache) int {
	return int(testutil.ToFloat64(cache.sizeGauge))
}

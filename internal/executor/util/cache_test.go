package util

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestMapPodCache_Add(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache(time.Minute, time.Second, "metric")
	cache.Add(pod)

	assert.Equal(t, pod, cache.Get("job1"))
}

func TestMapPodCache_Add_Expires(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache(time.Second/10, time.Second/100, "metric")
	cache.Add(pod)

	assert.Equal(t, pod, cache.Get("job1"))

	time.Sleep(time.Second / 5)

	assert.Equal(t, (*v1.Pod)(nil), cache.Get("job1"))
	assert.Equal(t, 0, len(cache.GetAll()))
}

func TestMapPodCache_AddIfNotExists(t *testing.T) {
	pod1 := makeManagedPod("job1")
	pod1.Name = "1"
	pod2 := makeManagedPod("job1")
	pod2.Name = "2"

	cache := NewMapPodCache(time.Minute, time.Second, "metric")
	assert.True(t, cache.AddIfNotExists(pod1))
	assert.False(t, cache.AddIfNotExists(pod2))
	assert.Equal(t, "1", cache.Get("job1").Name)
}

func TestMapPodCache_Update(t *testing.T) {
	pod1 := makeManagedPod("job1")
	pod1.Name = "1"
	pod2 := makeManagedPod("job1")
	pod2.Name = "2"

	cache := NewMapPodCache(time.Minute, time.Second, "metric")
	assert.False(t, cache.Update("job1", pod1))
	assert.Equal(t, 0, len(cache.GetAll()))
	cache.Add(pod1)
	assert.True(t, cache.Update("job1", pod2))
	assert.Equal(t, "2", cache.Get("job1").Name)
}

func TestMapPodCache_Delete(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache(time.Minute, time.Second, "metric2")

	cache.Add(pod)
	assert.NotNil(t, cache.Get("job1"))

	cache.Delete("job1")
	assert.Nil(t, cache.Get("job1"))
}

func TestMapPodCache_Delete_DoNotFailOnUnrecognisedKey(t *testing.T) {
	cache := NewMapPodCache(time.Minute, time.Second, "metric3")

	cache.Delete("madeupkey")
	assert.Nil(t, cache.Get("madeupkey"))
}

func TestNewMapPodCache_Get_ReturnsCopy(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache(time.Minute, time.Second, "metric4")

	cache.Add(pod)

	result := cache.Get("job1")
	assert.Equal(t, result, pod)

	pod.Namespace = "new value"
	assert.NotEqual(t, result, pod)
}

func TestMapPodCache_GetAll(t *testing.T) {
	pod1 := makeManagedPod("job1")
	pod2 := makeManagedPod("job2")
	cache := NewMapPodCache(time.Minute, time.Second, "metric5")

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
	pod := makeManagedPod("job1")
	cache := NewMapPodCache(time.Minute, time.Second, "metric6")

	cache.Add(pod)

	result := cache.GetAll()[0]
	assert.Equal(t, result, pod)

	pod.Namespace = "new value"
	assert.NotEqual(t, result, pod)
}

func makeManagedPod(jobId string) *v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{domain.JobId: jobId},
		},
	}
	return &pod
}

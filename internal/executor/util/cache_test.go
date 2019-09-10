package util

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestMapPodCache_Add(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache()
	cache.Add(pod)

	assert.Equal(t, pod, cache.Get("job1"))
}

func TestMapPodCache_Delete(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache()

	cache.Add(pod)
	assert.NotNil(t, cache.Get("job1"))

	cache.Delete("job1")
	assert.Nil(t, cache.Get("job1"))
}

func TestMapPodCache_Delete_DoNotFailOnUnrecognisedKey(t *testing.T) {
	cache := NewMapPodCache()

	cache.Delete("madeupkey")
	assert.Nil(t, cache.Get("madeupkey"))
}

func TestNewMapPodCache_Get_ReturnsCopy(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache()

	cache.Add(pod)

	result := cache.Get("job1")
	assert.Equal(t, result, pod)

	pod.Namespace = "new value"
	assert.NotEqual(t, result, pod)
}

func TestMapPodCache_GetAll(t *testing.T) {
	pod1 := makeManagedPod("job1")
	pod2 := makeManagedPod("job2")
	cache := NewMapPodCache()

	cache.Add(pod1)
	cache.Add(pod2)

	result := cache.GetAll()

	assert.Equal(t, len(result), 2)
	assert.NotNil(t, result["job1"])
	assert.NotNil(t, result["job2"])
}

func TestMapPodCache_GetReturnsACopy(t *testing.T) {
	pod := makeManagedPod("job1")
	cache := NewMapPodCache()

	cache.Add(pod)

	result := cache.GetAll()["job1"]
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

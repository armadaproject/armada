package util

import (
	v1 "k8s.io/api/core/v1"
	"sync"
)

type PodCache interface {
	Add(pod *v1.Pod)
	Delete(jobId string)
	Get(jobId string) *v1.Pod
	GetAll() map[string]*v1.Pod
}

type MapPodCache struct {
	cache  map[string]*v1.Pod
	rwLock sync.RWMutex
}

func NewMapPodCache() PodCache {
	return &MapPodCache{
		cache:  map[string]*v1.Pod{},
		rwLock: sync.RWMutex{},
	}
}

func (podCache *MapPodCache) Add(pod *v1.Pod) {
	jobId := ExtractJobId(pod)

	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	podCache.cache[jobId] = pod.DeepCopy()
}

func (podCache *MapPodCache) Delete(jobId string) {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	delete(podCache.cache, jobId)
}

func (podCache *MapPodCache) Get(jobId string) *v1.Pod {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	pod := podCache.cache[jobId]
	return pod.DeepCopy()
}

func (podCache *MapPodCache) GetAll() map[string]*v1.Pod {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	replica := make(map[string]*v1.Pod, len(podCache.cache))

	for k, v := range podCache.cache {
		replica[k] = v.DeepCopy()
	}
	return replica
}

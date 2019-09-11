package util

import (
	"github.com/G-Research/k8s-batch/internal/executor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	"sync"
)

type PodCache interface {
	Add(pod *v1.Pod)
	Delete(jobId string)
	Get(jobId string) *v1.Pod
	GetAll() map[string]*v1.Pod
}

var cacheSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: metrics.ArmadaExecutorMetricsPrefix + "submitted_job_cache_size",
		Help: "Number of jobs in the submitted job cache",
	},
)

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
	cacheSize.Inc()
}

func (podCache *MapPodCache) Delete(jobId string) {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	_, ok := podCache.cache[jobId]
	if ok {
		delete(podCache.cache, jobId)
		cacheSize.Dec()
	}
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

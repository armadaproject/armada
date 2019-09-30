package util

import (
	"github.com/G-Research/k8s-batch/internal/executor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	"runtime"
	"sync"
	"time"
)

type PodCache interface {
	Add(pod *v1.Pod)
	AddIfNotExists(pod *v1.Pod) bool
	Update(key string, pod *v1.Pod) bool
	Delete(jobId string)
	Get(jobId string) *v1.Pod
	GetAll() []*v1.Pod
}

type cacheRecord struct {
	pod    *v1.Pod
	expiry time.Time
}

type mapPodCache struct {
	records       map[string]cacheRecord
	rwLock        sync.RWMutex
	defaultExpiry time.Duration
	sizeGauge     prometheus.Gauge
}

func NewMapPodCache(expiry time.Duration, cleanUpInterval time.Duration, metricName string) PodCache {
	cache := &mapPodCache{
		records:       map[string]cacheRecord{},
		rwLock:        sync.RWMutex{},
		defaultExpiry: expiry,
		sizeGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + metricName + "_cache_size",
				Help: "Number of jobs in the submitted job cache",
			},
		),
	}
	cache.runCleanupLoop(cleanUpInterval)
	return cache
}

func (podCache *mapPodCache) Add(pod *v1.Pod) {
	jobId := ExtractJobId(pod)

	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	podCache.records[jobId] = cacheRecord{pod: pod.DeepCopy(), expiry: time.Now().Add(podCache.defaultExpiry)}
	podCache.sizeGauge.Inc()
}

func (podCache *mapPodCache) AddIfNotExists(pod *v1.Pod) bool {
	jobId := ExtractJobId(pod)

	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	existing, ok := podCache.records[jobId]
	exists := ok && existing.expiry.After(time.Now())
	if !exists {
		podCache.records[jobId] = cacheRecord{pod: pod.DeepCopy(), expiry: time.Now().Add(podCache.defaultExpiry)}
		podCache.sizeGauge.Inc()
	}
	return !exists
}

func (podCache *mapPodCache) Update(jobId string, pod *v1.Pod) bool {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	existing, ok := podCache.records[jobId]
	exists := ok && existing.expiry.After(time.Now())
	if exists {
		podCache.records[jobId] = cacheRecord{pod: pod.DeepCopy(), expiry: time.Now().Add(podCache.defaultExpiry)}
	}
	return ok
}

func (podCache *mapPodCache) Delete(jobId string) {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	_, ok := podCache.records[jobId]
	if ok {
		delete(podCache.records, jobId)
		podCache.sizeGauge.Dec()
	}
}

func (podCache *mapPodCache) Get(jobId string) *v1.Pod {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	record := podCache.records[jobId]
	if record.expiry.After(time.Now()) {
		return record.pod.DeepCopy()
	}
	return nil
}

func (podCache *mapPodCache) GetAll() []*v1.Pod {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	all := make([]*v1.Pod, 0, len(podCache.records))
	now := time.Now()

	for _, c := range podCache.records {
		if c.expiry.After(now) {
			all = append(all, c.pod.DeepCopy())
		}
	}
	return all
}

func (podCache *mapPodCache) deleteExpired() {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()
	now := time.Now()

	for id, c := range podCache.records {
		if c.expiry.Before(now) {
			delete(podCache.records, id)
			podCache.sizeGauge.Dec()
		}
	}
}

func (podCache *mapPodCache) runCleanupLoop(interval time.Duration) {
	stop := make(chan bool)
	runtime.SetFinalizer(podCache, func(podCache *mapPodCache) { stop <- true })
	go func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				podCache.deleteExpired()
			case <-stop:
				ticker.Stop()
				return
			}
		}
	}()
}

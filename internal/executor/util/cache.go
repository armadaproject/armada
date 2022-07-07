package util

import (
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/executor/metrics"
)

type PodCache interface {
	Add(pod *v1.Pod)
	AddIfNotExists(pod *v1.Pod) bool
	Update(key string, pod *v1.Pod) bool
	Delete(key string)
	Get(key string) *v1.Pod
	GetAll() []*v1.Pod
	GetKey(pod *v1.Pod) string
}

type cacheRecord struct {
	pod    *v1.Pod
	expiry time.Time
}

type mapPodCache struct {
	records       map[string]cacheRecord
	keyFunc       func(pod *v1.Pod) string
	rwLock        sync.RWMutex
	defaultExpiry time.Duration
	sizeGauge     prometheus.Gauge
}

func NewTimeExpiringPodCache(expiry time.Duration, cleanUpInterval time.Duration, metricName string, keyFunc func(pod *v1.Pod) string) *mapPodCache {
	cache := &mapPodCache{
		records:       map[string]cacheRecord{},
		keyFunc:       keyFunc,
		rwLock:        sync.RWMutex{},
		defaultExpiry: expiry,
		sizeGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + metricName + "_cache_size",
				Help: "Number of pods in the pod cache",
			},
		),
	}
	cache.runCleanupLoop(cleanUpInterval)
	return cache
}

func (podCache *mapPodCache) Add(pod *v1.Pod) {
	podId := podCache.GetKey(pod)

	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	podCache.records[podId] = cacheRecord{pod: pod.DeepCopy(), expiry: time.Now().Add(podCache.defaultExpiry)}
	podCache.sizeGauge.Set(float64(len(podCache.records)))
}

func (podCache *mapPodCache) AddIfNotExists(pod *v1.Pod) bool {
	podId := podCache.GetKey(pod)

	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	existing, ok := podCache.records[podId]
	exists := ok && existing.expiry.After(time.Now())
	if !exists {
		podCache.records[podId] = cacheRecord{pod: pod.DeepCopy(), expiry: time.Now().Add(podCache.defaultExpiry)}
		podCache.sizeGauge.Set(float64(len(podCache.records)))
	}
	return !exists
}

func (podCache *mapPodCache) Update(key string, pod *v1.Pod) bool {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	existing, ok := podCache.records[key]
	exists := ok && existing.expiry.After(time.Now())
	if exists {
		podCache.records[key] = cacheRecord{pod: pod.DeepCopy(), expiry: time.Now().Add(podCache.defaultExpiry)}
	}
	return ok
}

func (podCache *mapPodCache) Delete(key string) {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	_, ok := podCache.records[key]
	if ok {
		delete(podCache.records, key)
		podCache.sizeGauge.Set(float64(len(podCache.records)))
	}
}

func (podCache *mapPodCache) GetKey(pod *v1.Pod) string {
	return podCache.keyFunc(pod)
}

func (podCache *mapPodCache) Get(key string) *v1.Pod {
	podCache.rwLock.Lock()
	defer podCache.rwLock.Unlock()

	record := podCache.records[key]
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
		}
	}
	//Set size here, so it also fixes the value if it ever gets out of sync
	podCache.sizeGauge.Set(float64(len(podCache.records)))
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

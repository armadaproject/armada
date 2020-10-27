package service

import "sync"

type RetryCache interface {
	AddRetryAttempt(jobId string)
	GetNumberOfRetryAttempts(jobId string) int
	Evict(jobId string)
}

type InMemoryRetryCache struct {
	mutex *sync.RWMutex
	cache map[string]int
}

func NewInMemoryRetryCache() RetryCache {
	return &InMemoryRetryCache{
		mutex: &sync.RWMutex{},
		cache: make(map[string]int),
	}
}

func (rc *InMemoryRetryCache) AddRetryAttempt(jobId string) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.cache[jobId]++
}

func (rc *InMemoryRetryCache) GetNumberOfRetryAttempts(jobId string) int {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.cache[jobId]
}

func (rc *InMemoryRetryCache) Evict(jobId string) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	delete(rc.cache, jobId)
}

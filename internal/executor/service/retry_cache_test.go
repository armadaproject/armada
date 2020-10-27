package service

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryRetryCache_NewJobShouldHaveZeroRetries(t *testing.T) {
	retryCache := NewInMemoryRetryCache()

	assert.Zero(t, retryCache.GetNumberOfRetryAttempts("job-1"))
}

func TestInMemoryRetryCache_RetryAttemptsShouldBeRecorded(t *testing.T) {
	retryCache := NewInMemoryRetryCache()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < 10; i++ {
			retryCache.AddRetryAttempt("job-1")
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 15; i++ {
			retryCache.AddRetryAttempt("job-2")
		}
		wg.Done()
	}()

	wg.Wait()

	assert.Equal(t, 10, retryCache.GetNumberOfRetryAttempts("job-1"))
	assert.Equal(t, 15, retryCache.GetNumberOfRetryAttempts("job-2"))
}

func TestInMemoryRetryCache_JobRetriesAreEvicted(t *testing.T) {
	retryCache := NewInMemoryRetryCache()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < 7; i++ {
			retryCache.AddRetryAttempt("job-1")
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 10; i++ {
			retryCache.AddRetryAttempt("job-2")
		}
		wg.Done()
	}()

	wg.Wait()
	wg.Add(2)

	go func() {
		retryCache.Evict("job-1")
		wg.Done()
	}()

	go func() {
		retryCache.Evict("job-2")
		wg.Done()
	}()

	wg.Wait()

	assert.Zero(t, retryCache.GetNumberOfRetryAttempts("job-1"))
	assert.Zero(t, retryCache.GetNumberOfRetryAttempts("job-2"))
}

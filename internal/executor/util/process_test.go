package util

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessItemsWithThreadPool(t *testing.T) {
	input := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	output := []string{}
	outputMutex := &sync.Mutex{}

	ProcessItemsWithThreadPool(context.Background(), 2, input, func(item string) {
		outputMutex.Lock()
		defer outputMutex.Unlock()
		output = append(output, item)
	})

	assert.Len(t, output, len(input))
}

func TestProcessItemsWithThreadPool_HandlesContextCancellation(t *testing.T) {
	input := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	output := []string{}
	outputMutex := &sync.Mutex{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	ProcessItemsWithThreadPool(ctx, 2, input, func(item string) {
		time.Sleep(time.Millisecond * 70)
		outputMutex.Lock()
		defer outputMutex.Unlock()
		output = append(output, item)
	})

	// We process 2 items at a time, the context will timeout during the second call to the func
	assert.Len(t, output, 4)
}

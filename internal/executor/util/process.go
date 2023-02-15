package util

import (
	"context"
	"sync"

	commonUtil "github.com/armadaproject/armada/internal/common/util"
)

func ProcessItemsWithThreadPool[K any](ctx context.Context, maxThreadCount int, itemsToProcess []K, processFunc func(K)) {
	wg := &sync.WaitGroup{}
	processChannel := make(chan K)

	for i := 0; i < commonUtil.Min(len(itemsToProcess), maxThreadCount); i++ {
		wg.Add(1)
		go poolWorker(ctx, wg, processChannel, processFunc)
	}

	for _, item := range itemsToProcess {
		processChannel <- item
	}

	close(processChannel)
	wg.Wait()
}

func poolWorker[K any](ctx context.Context, wg *sync.WaitGroup, podsToProcess chan K, processFunc func(K)) {
	defer wg.Done()

	for pod := range podsToProcess {
		// Skip processing once context is finished
		if ctx.Err() != nil {
			continue
		}
		processFunc(pod)
	}
}

package fake

import (
	"sync"

	"github.com/G-Research/armada/internal/executor"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/fake/context"
)

func StartUp(config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return executor.StartUpWithContext(config, context.NewFakeClusterContext(config.Application.ClusterId), make([]chan bool, 0), wg)
}

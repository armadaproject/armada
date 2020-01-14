package fake

import (
	"sync"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/executor"
	"github.com/G-Research/armada/internal/executor/configuration"
)

func StartUp(config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return executor.StartUpWithContext(config, &fakeClusterContext{clusterId: config.Application.ClusterId, pods: map[string]*v1.Pod{}}, make([]chan bool, 0), wg)
}

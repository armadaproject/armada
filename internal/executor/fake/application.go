package fake

import (
	"sync"

	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/executor"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/fake/context"
	"github.com/G-Research/armada/internal/executor/metrics"
)

func StartUp(config configuration.ExecutorConfiguration, nodes []*context.NodeSpec) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return executor.StartUpWithContext(
		config,
		context.NewFakeClusterContext(config.Application, nodes),
		nil,
		task.NewBackgroundTaskManager(metrics.ArmadaExecutorMetricsPrefix),
		wg,
	)
}

package fake

import (
	"sync"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/task"
	"github.com/armadaproject/armada/internal/executor"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/fake/context"
	"github.com/armadaproject/armada/internal/executor/metrics"
)

func StartUp(ctx *armadacontext.Context, config configuration.ExecutorConfiguration, nodes []*context.NodeSpec) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return executor.StartUpWithContext(
		ctx,
		config,
		context.NewFakeClusterContext(config.Application, config.Kubernetes.NodeIdLabel, nodes),
		nil,
		task.NewBackgroundTaskManager(metrics.ArmadaExecutorMetricsPrefix),
		wg,
	)
}

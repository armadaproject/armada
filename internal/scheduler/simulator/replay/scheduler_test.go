package replay_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/simulator/replay"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestProductionSchedulerPreemptsAndResumesOfferedWork(t *testing.T) {
	const minute = int64(time.Minute / time.Microsecond)
	origin := testfixtures.BaseTime.UnixMicro()
	at := func(offset int64) int64 { return origin + offset*minute }

	cpu, gpu := resource.MustParse("1"), resource.MustParse("1")
	node := testfixtures.TestSchedulerObjectsNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
		"cpu": &cpu, "nvidia.com/gpu": &gpu,
	})
	node.Id, node.Name, node.Executor, node.Pool = "node", "node", "executor", testfixtures.TestPool
	executors := []*schedulerobjects.Executor{{
		Id: node.Executor, Pool: node.Pool, Nodes: []*schedulerobjects.Node{node},
	}}
	cfg := testfixtures.TestSchedulingConfig()
	cfg.PriorityClasses = map[string]types.PriorityClass{
		testfixtures.PriorityClass1: {Priority: 1, Preemptible: true},
		testfixtures.PriorityClass3: {Priority: 3, Preemptible: true},
	}
	cfg.DefaultPriorityClassName = testfixtures.PriorityClass1
	cfg.MaxSchedulingDuration = time.Second
	cfg.NodeIdLabel = configuration.NodeIdLabel
	queue := testfixtures.MakeTestQueue()
	ctx := context.Background()
	scheduler, err := replay.NewProductionScheduler(ctx, at(-90), cfg, executors, []*api.Queue{queue})
	require.NoError(t, err)

	job := func(id, priority string, offset, work int64, requests v1.ResourceList) replay.Job {
		return replay.Job{
			ID: id, Queue: queue.Name, PriorityClass: priority, SubmittedUS: at(offset),
			UsefulUS: work * minute, Requests: requests,
		}
	}
	cpuJob := job("cpu", testfixtures.PriorityClass1, -90, 180, v1.ResourceList{"cpu": cpu})
	gpuJob := job("gpu", testfixtures.PriorityClass1, -90, 180, v1.ResourceList{"nvidia.com/gpu": gpu})
	cpuJob.StartupUS, gpuJob.StartupUS, gpuJob.CheckpointUS = 45*minute, 45*minute, 60*minute
	preemptor := job("preemptor", testfixtures.PriorityClass3, 30, 10, v1.ResourceList{"cpu": cpu, "nvidia.com/gpu": gpu})

	out := &observations{}
	_, err = replay.Run(ctx, replay.Window{
		WarmupUS: at(-90), StartUS: at(0), EndUS: at(210), CycleUS: minute,
	}, readJobs([]replay.Job{cpuJob, gpuJob, preemptor}), scheduler, out)
	require.NoError(t, err)

	type attempt struct {
		job                                  string
		ordinal, start, end                  int64
		state                                replay.State
		startup, retained, lost, provisional int64
	}
	actual := make([]attempt, 0, len(out.attempts))
	for _, row := range out.attempts {
		require.Equal(t, "node", row.Placement.Node)
		require.NotEmpty(t, row.Placement.RunID)
		actual = append(actual, attempt{
			row.JobID, row.Ordinal, (row.AllocatedUS - origin) / minute,
			(row.EndUS - origin) / minute, row.State, row.StartupUS / minute,
			row.RetainedUS / minute, row.LostUS / minute, row.ProvisionalUS / minute,
		})
	}
	require.Equal(t, []attempt{
		{"cpu", 1, -90, 30, replay.Preempted, 45, 0, 75, 0},
		{"gpu", 1, -90, 30, replay.Preempted, 45, 60, 15, 0},
		{"preemptor", 1, 30, 40, replay.Completed, 0, 10, 0, 0},
		{"gpu", 2, 40, 205, replay.Completed, 45, 120, 0, 0},
		{"cpu", 2, 40, 210, replay.Running, 45, 0, 0, 125},
	}, actual)
}

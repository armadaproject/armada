package scheduler

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PoolAssigner allows jobs to be assigned to one or more pools
// Note that this is intended only for use with metrics calculation
type PoolAssigner interface {
	AssignPools(j *jobdb.Job) ([]string, error)
	Refresh(ctx *armadacontext.Context) error
}

type DefaultPoolAssigner struct {
	executorRepository database.ExecutorRepository
	executorById       map[string]*schedulerobjects.Executor
	nodeById           map[string]*schedulerobjects.Node
}

func NewPoolAssigner(executorRepository database.ExecutorRepository) *DefaultPoolAssigner {
	return &DefaultPoolAssigner{
		executorRepository: executorRepository,
		executorById:       map[string]*schedulerobjects.Executor{},
		nodeById:           map[string]*schedulerobjects.Node{},
	}
}

// Refresh updates executor state
func (p *DefaultPoolAssigner) Refresh(ctx *armadacontext.Context) error {
	executors, err := p.executorRepository.GetExecutors(ctx)
	if err != nil {
		return err
	}
	executorById := map[string]*schedulerobjects.Executor{}
	nodeById := map[string]*schedulerobjects.Node{}
	for _, e := range executors {
		executorById[e.Id] = e
		for _, node := range e.Nodes {
			nodeById[node.Id] = node
		}
	}
	p.executorById = executorById
	p.nodeById = nodeById
	return nil
}

// AssignPools returns the pools associated with the job or the empty string if no pool is valid
func (p *DefaultPoolAssigner) AssignPools(j *jobdb.Job) ([]string, error) {
	// If Job has an active run then use the pool associated with the executor it was assigned to
	if !j.Queued() && j.HasRuns() {
		pool := j.LatestRun().Pool()
		return []string{pool}, nil
	}
	// otherwise use the pools associated with the job
	return j.Pools(), nil
}

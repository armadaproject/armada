package scheduler

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// PoolAssigner allows jobs to be assigned to one or more pools
// Note that this is intended only for use with metrics calculation
type PoolAssigner interface {
	AssignPools(j *jobdb.Job) ([]string, error)
	Refresh(ctx *armadacontext.Context) error
}

type DefaultPoolAssigner struct {
	executorRepository database.ExecutorRepository
	poolByExecutorId   map[string]string
}

func NewPoolAssigner(executorRepository database.ExecutorRepository,
) (*DefaultPoolAssigner, error) {
	return &DefaultPoolAssigner{
		executorRepository: executorRepository,
		poolByExecutorId:   map[string]string{},
	}, nil
}

// Refresh updates executor state
func (p *DefaultPoolAssigner) Refresh(ctx *armadacontext.Context) error {
	executors, err := p.executorRepository.GetExecutors(ctx)
	if err != nil {
		return err
	}
	poolByExecutorId := map[string]string{}
	for _, e := range executors {
		poolByExecutorId[e.Id] = e.Pool
	}
	p.poolByExecutorId = poolByExecutorId
	return nil
}

// AssignPools returns the pools associated with the job or the empty string if no pool is valid
func (p *DefaultPoolAssigner) AssignPools(j *jobdb.Job) ([]string, error) {
	// If Job has an active run then use the pool associated with the executor it was assigned to
	if !j.Queued() && j.HasRuns() {
		pool := p.poolByExecutorId[j.LatestRun().Executor()]
		return []string{pool}, nil
	}
	// otherwise use the pools associated with the job
	return j.Pools(), nil
}

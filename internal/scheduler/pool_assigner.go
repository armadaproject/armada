package scheduler

import (
	"context"
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"
	"time"
)

type executor struct {
	nodeDb         *NodeDb
	minimumJobSize schedulerobjects.ResourceList
}

type PoolAssigner struct {
	executorTimeout    time.Duration
	priorityClasses    map[string]configuration.PriorityClass
	priorities         []int32
	indexedResources   []string
	indexedTaints      []string
	indexedNodeLabels  []string
	executorsByPool    map[string][]*executor
	executorRepository database.ExecutorRepository
	clock              clock.Clock
}

func (p *PoolAssigner) refresh(ctx context.Context) error {
	executors, err := p.executorRepository.GetExecutors(ctx)
	executorsByPool := map[string][]*executor{}
	if err != nil {
		return err
	}
	for _, e := range executors {
		if p.clock.Since(e.LastUpdateTime) < p.executorTimeout {
			nodeDb, err := p.constructNodeDb(e.Nodes)
			if err != nil {
				return errors.WithMessagef(err, "could not construct node db for executor %s", e.Id)
			}
			executorsByPool[e.Pool] = append(executorsByPool[e.Pool], &executor{
				nodeDb:         nodeDb,
				minimumJobSize: e.MinimumJobSize,
			})
		}
	}
	return nil
}

func (p *PoolAssigner) assignPool(j *jobdb.Job) (string, error) {
	req := PodRequirementFromJobSchedulingInfo(j.JobSchedulingInfo())
	for pool, executors := range p.executorsByPool {
		for _, e := range executors {
			nodeDb := e.nodeDb
			txn := nodeDb.Db.Txn(true)
			report, err := nodeDb.SelectNodeForPodWithTxn(txn, req)
			txn.Abort()
			if err != nil {
				return "", errors.WithMessagef(err, "error selecting node for job %s", j.Id())
			}
			if report.Node != nil {
				return pool, nil
			}
		}
	}
	return "", nil
}

func (srv *PoolAssigner) constructNodeDb(nodes []*schedulerobjects.Node) (*NodeDb, error) {
	// Nodes to be considered by the scheduler.
	nodeDb, err := NewNodeDb(
		srv.priorityClasses,
		srv.indexedResources,
		srv.indexedTaints,
		srv.indexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	err = nodeDb.UpsertMany(nodes)
	if err != nil {
		return nil, err
	}
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}

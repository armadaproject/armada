package jobqueue

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type EvictedJobQueue struct{}

func NewEvictedJobQueue(queuedJobs model.JobQueue, evictedJobs map[string]*context.JobSchedulingContext) *EvictedJobQueue {
	return &EvictedJobQueue{}
}

func (jq *EvictedJobQueue) Next() *context.GangSchedulingContext {
	return nil
}

func (jq *EvictedJobQueue) UpdateQueueCost() {

}

func (jq *EvictedJobQueue) SetOnlyYieldEvicted() {

}

func (jq *EvictedJobQueue) SetOnlyYieldEvictedForQueue(string) {

}

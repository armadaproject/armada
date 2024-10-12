package scheduling2

import (
	"container/heap"
	"github.com/armadaproject/armada/internal/common/queues"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/pkg/errors"
)

type JobQueue struct {
	iteratorsByQueue map[string]*scheduling.QueuedGangIterator
	pq               *queues.PriorityQueue
	queueRepository  fairness.QueueRepository
	buffer           schedulerobjects.ResourceList
	costProvider     fairness.FairnessCostProvider
}

func NewJobQueue(iteratorsByQueue map[string]*scheduling.QueuedGangIterator) (*JobQueue, error) {
	var jq = &JobQueue{
		iteratorsByQueue: nil,
	}
	for queue, queueIt := range iteratorsByQueue {
		gctx, err := queueIt.Peek()
		if err != nil {
			return nil, err
		}
	}
	return jq
}

func (j *JobQueue) Next() (*context.GangSchedulingContext, error) {
	for j.pq.Len() > 0 {
		queueDetails := heap.Pop(j.pq).(*queues.Item)
		queueIter := j.iteratorsByQueue[queueDetails.Value]
		gctx, err := queueIter.Next()
		if err != nil {
			return nil, err
		}
		if gctx != nil {
			j.UpdateQueueCost()
			return gctx, nil
		}
	}
	return nil, nil
}

func (j *JobQueue) UpdateQueueCost(queueName string) error {
	queue, ok := j.queueRepository.GetQueue(queueName)
	if !ok {
		return errors.Errorf("unknown queue %s", queueName)
	}
	gctx, err := j.iteratorsByQueue[queueName].Peek()
	if err != nil {
		return err
	}

	j.buffer.Zero()
	j.buffer.Add(queue.GetAllocation())
	j.buffer.Add(gctx.TotalResourceRequests)
	cost := j.costProvider.WeightedCostFromAllocation(j.buffer, queue.GetWeight())
	heap.Push(j.pq, &queues.Item{
		Value:    queue,
		Priority: cost,
	})
}

func (j *JobQueue) SetOnlyYieldEvicted() {}

func (j *JobQueue) SetOnlyYieldEvictedForQueue(string) {}

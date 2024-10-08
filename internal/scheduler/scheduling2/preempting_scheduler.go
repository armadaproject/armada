package scheduling2

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/jobqueue"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

// PreemptingScheduler wraps a QueueScheduler. Its role is to identify running jobs eligible for preemption and then
// create a new job queue, made up of these evicted jobs, along with all queued jobs. The actual scheduling of this
// new queue is performed by the QueueScheduler
type PreemptingScheduler struct {
	queueScheduler        *QueueScheduler
	fairShareEvictor      FairShareEvictor
	oversubscribedEvictor OversubscribedEvictor
}

func NewPreemptingScheduler(queueScheduler *QueueScheduler) *PreemptingScheduler {
	return &PreemptingScheduler{
		queueScheduler:        queueScheduler,
		fairShareEvictor:      FairShareEvictor{},
		oversubscribedEvictor: OversubscribedEvictor{},
	}
}

func (s *PreemptingScheduler) Schedule(schedCtx *context.SchedulingContext, queue model.JobQueue) error {

	// Evict any jobs that are over their fair share.  Note that these are not yet preempted, rather we put them back
	// in the queue to try and reschedule them. If they are not rescheduled then they are preempted
	fairShareEvictor := FairShareEvictor{}
	evictedJobs := fairShareEvictor.Evict(nil, schedCtx, 1.0)
	evictedJobQueue := jobqueue.NewEvictedJobQueue(queue, evictedJobs)

	err := s.queueScheduler.Schedule(schedCtx, evictedJobQueue, evictedJobs)
	if err != nil {
		return err
	}

	// Now run a second eviction. This will evict all jobs where the node is oversubscribed
	oversubscribedEvictor := &OversubscribedEvictor{}
	evictedJobs = oversubscribedEvictor.Evict(nil)
	evictedJobQueue := jobqueue.NewEvictedJobQueue(queue, evictedJobs)

	err = s.queueScheduler.Schedule(schedCtx, evictedJobQueue, evictedJobs)
	if err != nil {
		return err
	}

	return nil
}

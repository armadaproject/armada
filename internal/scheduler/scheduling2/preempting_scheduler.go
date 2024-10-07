package scheduling2

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/jobqueue"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type PreemptingScheduler struct {
	queueScheduler *QueueScheduler
}

func NewPreemptingScheduler(queueScheduler *QueueScheduler) *PreemptingScheduler {
	return &PreemptingScheduler{
		queueScheduler: queueScheduler,
	}
}

func (s *PreemptingScheduler) Schedule(schedCtx *context.SchedulingContext, queue model.JobQueue) error {

	// Evict any jobs that are over their fair share.  Note that these are not yet preempted,
	// rather we put them back in the queue to try and reschedule them.  If they are not rescheduled
	// then they are preempted
	fairShareEvictor := FairShareEvictor{}
	evictedJobs := fairShareEvictor.Evict(nil)
	evictedJobQueue := jobqueue.NewEvictedJobQueue(queue, evictedJobs)

	err := s.queueScheduler.Schedule(schedCtx, evictedJobQueue, evictedJobs)
	if err != nil {
		return err
	}

	// Now run a second eviction. This will evict all jobs where the node is oversubscribed
	oversubscribedEvictor := &OversubscribedEvictor{}
	evictedJobs = oversubscribedEvictor.Evict(nil)
	evictedJobQueue := jobqueue.NewEvictedJobQueue(queue, evictedJobs)

	err = s.queueScheduler.Schedule(schedCtx, queue, evictedJobs)
	if err != nil {
		return err
	}

	return nil
}

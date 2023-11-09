package jobdb

import (
	"time"

	"github.com/armadaproject/armada/internal/scheduler/interfaces"
)

type JobPriorityComparer struct{}

// // Compare jobs first by priority, then by submittedTime, and finally by jobDbCreatedIndex.
// // Returns -1 if a comes before b and 1 otherwise. Jobs are guaranteed to be totally ordered.
// func (j JobPriorityComparer) Compare(a, b *Job) int {
// 	// Compare the jobs by priority.
// 	// TODO: We should compare first by priorityClassPriority.
// 	if a.priority != b.priority {
// 		if a.priority < b.priority {
// 			return -1
// 		} else {
// 			return 1
// 		}
// 	}

// 	// If the jobs have the same priority, compare them by submittedTime time.
// 	if a.submittedTime != b.submittedTime {
// 		if a.submittedTime < b.submittedTime {
// 			return -1
// 		} else {
// 			return 1
// 		}
// 	}

// 	// Tie-break by logical creation timestamp.
// 	if a.id < b.id {
// 		return -1
// 	} else {
// 		return 1
// 	}
// }

type JobQueueTtlComparer struct{}

// Compare jobs by their remaining queue time before expiry
// Invariants:
//   - Job.queueTtl must be > 0
//   - Job.created must be < `t`
func (j JobQueueTtlComparer) Compare(a, b *Job) int {
	// Jobs with equal id are always considered equal.
	// This ensures at most one job with a particular id can exist in the jobDb.
	if a.id == b.id {
		return 0
	}

	// TODO: Calling time.Now() here doesn't sound right. We should probably sort by earliest expiry time.
	timeSeconds := time.Now().UTC().Unix()
	aDuration := timeSeconds - (a.submittedTime / 1_000_000_000)
	bDuration := timeSeconds - (b.submittedTime / 1_000_000_000)

	aRemaining := max(0, a.GetQueueTtlSeconds()-aDuration)
	bRemaining := max(0, b.GetQueueTtlSeconds()-bDuration)

	// If jobs have different ttl remaining, they are ordered by remaining queue ttl - the smallest ttl first.
	if aRemaining != bRemaining {
		if aRemaining < bRemaining {
			return -1
		} else {
			return 1
		}
	}

	// Tie-break by logical creation timestamp.
	if a.id < b.id {
		return -1
	} else if a.id > b.id {
		return 1
	}
	panic("We should never get here. Since we check for job id equality at the top of this function.")
}

func max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

// type ShouldBeScheduledBeforeComparer struct{}

// // Compare returns -1 if a should be scheduled before b and 1 otherwise.
// func (ShouldBeScheduledBeforeComparer) Compare(a, b *Job) int {
// 	if ShouldBeScheduledBefore(a, b) {
// 		return -1
// 	} else {
// 		return 1
// 	}
// }

// type ShouldBePreemptedBeforeComparer struct{}

// // Compare returns -1 if a should be preempted before b and 1 otherwise.
// func (ShouldBePreemptedBeforeComparer) Compare(a, b *Job) int {
// 	if ShouldBePreemptedBefore(a, b) {
// 		return -1
// 	} else {
// 		return 1
// 	}
// }

func (JobPriorityComparer) Compare(job, other *Job) int {
	return Compare(job, other)
}

// Compare defines the order in which jobs in a particular queue should be scheduled,
func (job *Job) Compare(other interfaces.LegacySchedulerJob) int {
	// We need this cast for now to expose this method via an interface.
	// This is safe since we only ever compare jobs of the same type.
	return Compare(job, other.(*Job))
}

// Compare defines the order in which jobs in a particular queue should be scheduled,
// both when scheduling new jobs and when re-scheduling evicted jobs.
// Specifically, compare returns
//   - 0 if the jobs have equal job id,
//   - -1 if job should be scheduled before other,
//   - +1 if other should be scheduled before other.
func Compare(job, other *Job) int {
	// Jobs with equal id are always considered equal.
	// This ensures at most one job with a particular id can exist in the jobDb.
	if job.id == other.id {
		return 0
	}

	// Running jobs always come before queued jobs.
	// This to ensure evicted jobs are rescheduled before scheduling new jobs.
	jobIsActive := job.activeRun != nil && !job.activeRun.InTerminalState()
	otherIsActive := other.activeRun != nil && !other.activeRun.InTerminalState()
	if jobIsActive && !otherIsActive {
		return -1
	} else if !jobIsActive && otherIsActive {
		return 1
	}

	// PriorityClassPriority indicates urgency.
	// Hence, jobs of higher priorityClassPriority come first.
	if job.priorityClass.Priority > other.priorityClass.Priority {
		return -1
	} else if job.priorityClass.Priority < other.priorityClass.Priority {
		return 1
	}

	// Jobs higher in queue-priority come first.
	if job.priority < other.priority {
		return -1
	} else if job.priority > other.priority {
		return 1
	}

	// If both jobs are active, order by time since the job was scheduled.
	// This ensures jobs that have been running for longer are rescheduled first,
	// which reduces wasted compute time when preempting.
	if jobIsActive && otherIsActive {
		if job.activeRunTimestamp < other.activeRunTimestamp {
			return -1
		} else if job.activeRunTimestamp > other.activeRunTimestamp {
			return 1
		}
	}

	// Jobs that have been queuing for longer are scheduled first.
	if job.submittedTime < other.submittedTime {
		return -1
	} else if job.submittedTime > other.submittedTime {
		return 1
	}

	// Tie-break by jobId, which must be unique.
	// This ensure there is a total order between jobs, i.e., no jobs are equal from an ordering point of view.
	if job.id < other.id {
		return -1
	} else if job.id > other.id {
		return 1
	}
	panic("We should never get here. Since we check for job id equality at the top of this function.")
}

// // ShouldBeScheduledBefore returns true if job should be scheduled before other.
// func (job *Job) ShouldBeScheduledBefore(other interfaces.LegacySchedulerJob) bool {
// 	// We need this cast for now to expose this method via an interface.
// 	// This is safe since we only ever compare jobs of the same type.
// 	return ShouldBeScheduledBefore(job, other.(*Job))
// }

// // ShouldBePreemptedBefore returns true if job should be preempted before other.
// func (job *Job) ShouldBePreemptedBefore(other interfaces.LegacySchedulerJob) bool {
// 	// We need this cast for now to expose this method via an interface.
// 	// This is safe since we only ever compare jobs of the same type.
// 	return ShouldBePreemptedBefore(job, other.(*Job))
// }

// // ShouldBeScheduledBefore returns true if job should be scheduled before other.
// func ShouldBeScheduledBefore(job, other *Job) bool {
// 	if job.priorityClass.Priority > other.priorityClass.Priority {
// 		return true
// 	} else if job.priorityClass.Priority < other.priorityClass.Priority {
// 		return false
// 	}
// 	if job.priority < other.priority {
// 		return true
// 	} else if job.priority > other.priority {
// 		return false
// 	}
// 	if job.submittedTime < other.submittedTime {
// 		return true
// 	} else if job.submittedTime > other.submittedTime {
// 		return false
// 	}
// 	if job.id < other.id {
// 		return true
// 	} else {
// 		return false
// 	}
// }

// // ShouldBePreemptedBefore returns true if job should be preempted before other.
// func ShouldBePreemptedBefore(job, other *Job) bool {
// 	if job.priorityClass.Priority > other.priorityClass.Priority {
// 		return false
// 	} else if job.priorityClass.Priority < other.priorityClass.Priority {
// 		return true
// 	}
// 	if job.priority < other.priority {
// 		return false
// 	} else if job.priority > other.priority {
// 		return true
// 	}
// 	if job.activeRunTimestamp < other.activeRunTimestamp {
// 		return false
// 	} else if job.activeRunTimestamp > other.activeRunTimestamp {
// 		return true
// 	}
// 	if job.submittedTime < other.submittedTime {
// 		return false
// 	} else if job.submittedTime > other.submittedTime {
// 		return true
// 	}
// 	if job.id < other.id {
// 		return false
// 	} else {
// 		return true
// 	}
// }

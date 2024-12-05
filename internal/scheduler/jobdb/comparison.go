package jobdb

type (
	JobPriorityComparer       struct{}
	MarketJobPriorityComparer struct{}
	JobIdHasher               struct{}
)

func (JobIdHasher) Hash(j *Job) uint32 {
	var hash uint32
	for _, c := range j.id {
		hash = 31*hash + uint32(c)
	}
	return hash
}

func (JobIdHasher) Equal(a, b *Job) bool {
	return a == b
}

func (JobPriorityComparer) Compare(job, other *Job) int {
	return SchedulingOrderCompare(job, other)
}

func (MarketJobPriorityComparer) Compare(job, other *Job) int {
	return MarketSchedulingOrderCompare(job, other)
}

// SchedulingOrderCompare defines the order in which jobs in a particular queue should be scheduled,
func (job *Job) SchedulingOrderCompare(other *Job) int {
	// We need this cast for now to expose this method via an interface.
	// This is safe since we only ever compare jobs of the same type.
	return SchedulingOrderCompare(job, other)
}

// SchedulingOrderCompare defines the order in which jobs in a queue should be scheduled
// (both when scheduling new jobs and when re-scheduling evicted jobs).
// Specifically, compare returns
//   - 0 if the jobs have equal job id,
//   - -1 if job should be scheduled before other,
//   - +1 if other should be scheduled before other.
func SchedulingOrderCompare(job, other *Job) int {
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
	// This ensures there is a total order between jobs, i.e., no jobs are equal from an ordering point of view.
	if job.id < other.id {
		return -1
	} else if job.id > other.id {
		return 1
	}
	panic("We should never get here. Since we check for job id equality at the top of this function.")
}

func MarketSchedulingOrderCompare(job, other *Job) int {
	// Jobs with equal id are always considered equal.
	// This ensures at most one job with a particular id can exist in the jobDb.
	if job.id == other.id {
		return 0
	}

	// Next we sort on bidPrice
	if job.bidPrice > other.bidPrice {
		return -1
	} else if job.bidPrice < other.bidPrice {
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
	jobIsActive := job.activeRun != nil && !job.activeRun.InTerminalState()
	otherIsActive := other.activeRun != nil && !other.activeRun.InTerminalState()
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
	// This ensures there is a total order between jobs, i.e., no jobs are equal from an ordering point of view.
	if job.id < other.id {
		return -1
	} else if job.id > other.id {
		return 1
	}
	panic("We should never get here. Since we check for job id equality at the top of this function.")
}

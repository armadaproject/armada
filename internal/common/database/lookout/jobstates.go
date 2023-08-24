package lookout

import "github.com/armadaproject/armada/internal/common/util"

type JobState string

type JobRunState string

const (
	JobQueued    JobState = "QUEUED"
	JobPending   JobState = "PENDING"
	JobRunning   JobState = "RUNNING"
	JobSucceeded JobState = "SUCCEEDED"
	JobFailed    JobState = "FAILED"
	JobCancelled JobState = "CANCELLED"
	JobPreempted JobState = "PREEMPTED"
	JobLeased    JobState = "LEASED"

	JobQueuedOrdinal    = 1
	JobPendingOrdinal   = 2
	JobRunningOrdinal   = 3
	JobSucceededOrdinal = 4
	JobFailedOrdinal    = 5
	JobCancelledOrdinal = 6
	JobPreemptedOrdinal = 7
	JobLeasedOrdinal    = 8

	JobRunLeased           JobRunState = "RUN_LEASED"
	JobRunPending          JobRunState = "RUN_PENDING"
	JobRunRunning          JobRunState = "RUN_RUNNING"
	JobRunSucceeded        JobRunState = "RUN_SUCCEEDED"
	JobRunFailed           JobRunState = "RUN_FAILED"
	JobRunTerminated       JobRunState = "RUN_TERMINATED"
	JobRunPreempted        JobRunState = "RUN_PREEMPTED"
	JobRunUnableToSchedule JobRunState = "RUN_UNABLE_TO_SCHEDULE"
	JobRunLeaseReturned    JobRunState = "RUN_LEASE_RETURNED"
	JobRunLeaseExpired     JobRunState = "RUN_LEASE_EXPIRED"
	JobRunMaxRunsExceeded  JobRunState = "RUN_MAX_RUNS_EXCEEDED"

	JobRunPendingOrdinal          = 1
	JobRunRunningOrdinal          = 2
	JobRunSucceededOrdinal        = 3
	JobRunFailedOrdinal           = 4
	JobRunTerminatedOrdinal       = 5
	JobRunPreemptedOrdinal        = 6
	JobRunUnableToScheduleOrdinal = 7
	JobRunLeaseReturnedOrdinal    = 8
	JobRunLeaseExpiredOrdinal     = 9
	JobRunMaxRunsExceededOrdinal  = 10
	JobRunLeasedOrdinal           = 11
)

var (
	// JobStates is an ordered list of states
	JobStates = []JobState{
		JobQueued,
		JobLeased,
		JobPending,
		JobRunning,
		JobSucceeded,
		JobFailed,
		JobCancelled,
		JobPreempted,
	}

	JobStateMap = map[int]JobState{
		JobLeasedOrdinal:    JobLeased,
		JobQueuedOrdinal:    JobQueued,
		JobPendingOrdinal:   JobPending,
		JobRunningOrdinal:   JobRunning,
		JobSucceededOrdinal: JobSucceeded,
		JobFailedOrdinal:    JobFailed,
		JobCancelledOrdinal: JobCancelled,
		JobPreemptedOrdinal: JobPreempted,
	}

	JobStateOrdinalMap = util.InverseMap(JobStateMap)

	JobRunStateMap = map[int]JobRunState{
		JobRunLeasedOrdinal:           JobRunLeased,
		JobRunPendingOrdinal:          JobRunPending,
		JobRunRunningOrdinal:          JobRunRunning,
		JobRunSucceededOrdinal:        JobRunSucceeded,
		JobRunFailedOrdinal:           JobRunFailed,
		JobRunTerminatedOrdinal:       JobRunTerminated,
		JobRunPreemptedOrdinal:        JobRunPreempted,
		JobRunUnableToScheduleOrdinal: JobRunUnableToSchedule,
		JobRunLeaseReturnedOrdinal:    JobRunLeaseReturned,
		JobRunLeaseExpiredOrdinal:     JobRunLeaseExpired,
		JobRunMaxRunsExceededOrdinal:  JobRunMaxRunsExceeded,
	}
)

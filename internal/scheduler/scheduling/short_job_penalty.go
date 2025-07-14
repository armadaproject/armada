package scheduling

import (
	"time"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// Used to penalize short-running jobs by pretending they
// ran for some minimum length when calculating costs.
type ShortJobPenalty struct {
	cutoffDurationByPool map[string]time.Duration
	now                  time.Time
}

func NewShortJobPenalty(cutoffDurationByPool map[string]time.Duration) *ShortJobPenalty {
	return &ShortJobPenalty{
		cutoffDurationByPool: cutoffDurationByPool,
	}
}

func (sjp *ShortJobPenalty) SetNow(now time.Time) {
	if sjp == nil {
		return
	}
	sjp.now = now
}

func (sjp *ShortJobPenalty) ShouldApplyPenalty(job *jobdb.Job) bool {
	if sjp == nil || sjp.now.IsZero() {
		return false
	}

	if !job.InTerminalState() {
		return false
	}

	jobRun := job.LatestRun()
	if jobRun == nil {
		return false
	}

	if jobRun.Preempted() || jobRun.PreemptRequested() || jobRun.PreemptedTime() != nil {
		return false
	}

	jobStart := jobRun.RunningTime()
	if jobStart == nil {
		return false
	}

	return sjp.now.Sub(*jobStart) < sjp.cutoffDurationByPool[jobRun.Pool()]
}

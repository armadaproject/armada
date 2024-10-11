package context

import "github.com/armadaproject/armada/internal/scheduler/jobdb"

func CalculateAwayQueueName(queueName string) string {
	return queueName + "-away"
}

func IsHomeJob(job *jobdb.Job, currentPool string) bool {
	// Away jobs  can never have been scheduled in this round
	// and therefore must have an active run
	if job.Queued() || job.LatestRun() == nil {
		return true
	}
	return job.LatestRun().Pool() == currentPool
}

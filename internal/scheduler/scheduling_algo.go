package scheduler

import (
	"github.com/hashicorp/go-memdb"
)

// SchedulingAlgo is an interface that should bne implemented by structs capable of assigning Jobs to nodes
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided
	// It should return a slcie containing all scheduled jobs
	Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error)
}

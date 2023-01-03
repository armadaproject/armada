package scheduler

import (
	"github.com/hashicorp/go-memdb"
)

type SchedulingAlgo interface {
	Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error)
}

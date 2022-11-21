package scheduler

import (
	"strings"
	"sync"

	"golang.org/x/exp/maps"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

type SubmitChecker struct {
	nodeDbByExecutor map[string]*NodeDb
	mu               sync.Mutex
}

// RegisterNodeDb adds a NodeDb to use when checking if a pod can be scheduled.
// To only check static scheduling requirements, set NodeDb.CheckOnlyStaticRequirements = true
// before registering it.
func (srv *SubmitChecker) RegisterNodeDb(executor string, nodeDb *NodeDb) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.nodeDbByExecutor == nil {
		srv.nodeDbByExecutor = make(map[string]*NodeDb)
	}
	srv.nodeDbByExecutor[executor] = nodeDb
}

// Check if a set of pods can be scheduled onto some cluster.
func (srv *SubmitChecker) Check(reqs []*schedulerobjects.PodRequirements) (bool, string) {
	if len(srv.nodeDbByExecutor) == 0 {
		return false, "no executor clusters available"
	}
	canSchedule := false
	var sb strings.Builder

	// Make a shallow copy to avoid holding the lock and
	// preventing registering new NodeDbs while checking if jobs can be scheduled.
	srv.mu.Lock()
	nodeDbByExecutor := maps.Clone(srv.nodeDbByExecutor)
	srv.mu.Unlock()
	for executor, nodeDb := range nodeDbByExecutor {
		reports, ok, err := nodeDb.ScheduleManyWithTxn(nodeDb.db.Txn(false), reqs)
		sb.WriteString(executor)
		sb.WriteString("\n")
		if err != nil {
			sb.WriteString(err.Error())
			sb.WriteString("\n")
		} else {
			canSchedule = canSchedule || ok
			for _, report := range reports {
				sb.WriteString(report.String())
				sb.WriteString("\n")
			}
		}
		sb.WriteString("---")
		sb.WriteString("\n")
	}
	return canSchedule, sb.String()
}

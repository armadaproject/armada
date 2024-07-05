package scheduler

import (
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	log "github.com/sirupsen/logrus"
)

// TODO Remove this and just use node.GetPool() once we have migrated to have all nodes have pool set
// We may still want to keep a "fallback" pool, which may be a statically defined default pool or remain at executor level
func GetNodePool(node *schedulerobjects.Node, executor *schedulerobjects.Executor) string {
	if node != nil {
		if node.GetPool() != "" {
			return node.GetPool()
		} else {
			log.Warnf("node %s does not have a pool set, defaulting to cluster pool", node.Id)
		}
	}

	if executor == nil {
		return ""
	}
	return executor.GetPool()
}

// TODO Remove this and just use run.NodePool() once we have migrated to have all runs have node pool set
func GetRunPool(run *jobdb.JobRun, node *schedulerobjects.Node, executor *schedulerobjects.Executor) string {
	if run.NodePool() != "" {
		return run.NodePool()
	}
	return GetNodePool(node, executor)
}

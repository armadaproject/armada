package scheduling

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type NodeScheduler struct {
	jobDb        JobRepository
	nodeDb       *nodedb.NodeDb
	defragConfig configuration.DefragConfig
}

func NewNodeScheduler(jobDb JobRepository, nodeDb *nodedb.NodeDb, defragConfig configuration.DefragConfig) *NodeScheduler {
	return &NodeScheduler{
		nodeDb:       nodeDb,
		jobDb:        jobDb,
		defragConfig: defragConfig,
	}
}

// TODO also need current queue costs
func (n *NodeScheduler) Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext) (bool, string, error) {
	nodes, err := n.nodeDb.GetNodes()
	fmt.Println(err)

	for _, jctx := range gctx.JobSchedulingContexts {
		for _, node := range nodes {
			if node.AllocatableByPriority[internaltypes.EvictedPriority].Exceeds(jctx.Job.AllResourceRequirements()) {

			}
			//	 Keep kicking off jobs until the job fits or we can't kick off more job
			//   Determine the order these jobs should be kicked off in
			//   Determine the cost of kicking off the jobs

			// If we find a node that is "free" exit it early
			// Sort nodes by cost
			// Average age of evicted jobs
			// Maximum % impact on queue
			// Tie break on queue name

		}

	}

	return false, "no scheduling done", nil
}

package pricer

import (
	"fmt"
	"sort"

	"k8s.io/utils/set"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

const (
	GangUniformityLabelIsNotIndexedUnschedulableReason = "uniformity label is not indexed"
	GangNoNodesWithUniformityLabelUnschedulableReason  = "no nodes with uniformity label"
)

type GangPricingResult struct {
	Evaluated           bool
	Schedulable         bool
	Price               float64
	UnschedulableReason string
}

type GangPricer struct {
	nodeScheduler NodeScheduler
	jobDb         jobdb.JobRepository
	nodeDb        *nodedb.NodeDb
}

func NewGangPricer(
	nodeScheduler NodeScheduler,
	jobDb jobdb.JobRepository,
	nodeDb *nodedb.NodeDb,
) *GangPricer {
	return &GangPricer{
		nodeScheduler: nodeScheduler,
		nodeDb:        nodeDb,
		jobDb:         jobDb,
	}
}

// Price determines the lowest price across all nodes at which the provided gang can be scheduled at. This method
// has no side effects and utilises the state provided at GangPricer instantiation.
func (gp *GangPricer) Price(gctx *context.GangSchedulingContext) (GangPricingResult, error) {
	nodes, err := gp.nodeDb.GetNodes()
	if err != nil {
		return GangPricingResult{
			Evaluated:           false,
			Schedulable:         false,
			Price:               0,
			UnschedulableReason: "",
		}, err
	}

	if isValid, reason := gp.isValidNodeUniformityLabel(gctx.NodeUniformity); !isValid {
		return GangPricingResult{
			Evaluated:           true,
			Schedulable:         false,
			Price:               0,
			UnschedulableReason: reason,
		}, nil
	}

	nodesByNodeUniformityLabel := gp.groupNodesByNodeUniformityLabel(gctx.NodeUniformity, nodes)
	schedulingCandidates := make([]*schedulingResult, 0, len(nodesByNodeUniformityLabel))

	for _, groupedNodes := range nodesByNodeUniformityLabel {
		result, err := gp.scheduleOnNodes(gctx, groupedNodes)
		if err != nil {
			return GangPricingResult{
				Evaluated:           false,
				Schedulable:         false,
				Price:               0,
				UnschedulableReason: "",
			}, err
		}
		if result.scheduled {
			schedulingCandidates = append(schedulingCandidates, result)
		}
	}

	if len(schedulingCandidates) == 0 {
		reason := constraints.JobDoesNotFitUnschedulableReason
		if gctx.Cardinality() > 1 {
			reason = constraints.GangDoesNotFitUnschedulableReason
		}
		return GangPricingResult{
			Evaluated:           true,
			Schedulable:         false,
			Price:               0,
			UnschedulableReason: reason,
		}, nil
	}

	sort.Sort(schedulingCostOrder(schedulingCandidates))
	selectedCandidate := schedulingCandidates[0]
	return GangPricingResult{
		Evaluated:           true,
		Schedulable:         true,
		Price:               selectedCandidate.schedulingCost,
		UnschedulableReason: "",
	}, nil
}

// scheduleOnNodes
// The function attempts to schedule a gang onto the nodes provided
// It uses the nodeScheduler to score each node and picks the best one based on cost, which is derived from price
func (gp *GangPricer) scheduleOnNodes(gctx *context.GangSchedulingContext, nodes []*internaltypes.Node) (*schedulingResult, error) {
	combinedResult := &schedulingResult{
		scheduled:      true,
		schedulingCost: 0,
		results:        make([]*NodeSchedulingResult, 0, len(gctx.JobSchedulingContexts)),
	}

	gangJobIds := set.New(slices.Map(gctx.JobSchedulingContexts, func(jctx *context.JobSchedulingContext) string {
		return jctx.JobId
	})...)

	for _, jctx := range gctx.JobSchedulingContexts {
		candidateNodes := make([]*NodeSchedulingResult, 0, len(nodes))
		for _, node := range nodes {
			result, err := gp.nodeScheduler.Schedule(jctx, node, gangJobIds)
			if err != nil {
				return nil, err
			}

			if !result.scheduled {
				continue
			}

			candidateNodes = append(candidateNodes, result)
			// No preemption required - this is the ideal result - exit early
			if result.price == 0 {
				break
			}
		}

		if len(candidateNodes) == 0 {
			return &schedulingResult{scheduled: false}, nil
		}

		sort.Sort(nodeCostOrder(candidateNodes))
		selectedCandidate := candidateNodes[0]
		combinedResult.results = append(combinedResult.results, selectedCandidate)
		combinedResult.schedulingCost = max(combinedResult.schedulingCost, selectedCandidate.price)

		// update node scheduled on + scheduling context, to reflect current jctx being scheduled
		err := gp.updateState(selectedCandidate, nodes)
		if err != nil {
			return nil, err
		}
	}

	return combinedResult, nil
}

func (gp *GangPricer) updateState(result *NodeSchedulingResult, nodes []*internaltypes.Node) error {
	updatedNode := result.node.DeepCopyNilKeys()
	jobsToPreempt := make([]*jobdb.Job, 0, len(result.jobIdsToPreempt))

	for _, jobId := range result.jobIdsToPreempt {
		job := gp.jobDb.GetById(jobId)
		if job == nil {
			return fmt.Errorf("failed to find job %s in job db", jobId)
		}
		if job.InTerminalState() {
			return fmt.Errorf("job %s in terminal state", jobId)
		}
		jobsToPreempt = append(jobsToPreempt, job)
	}

	updatedNode, err := gp.nodeDb.UnbindJobsFromNode(jobsToPreempt, updatedNode)
	if err != nil {
		return err
	}
	updatedNode, err = gp.nodeDb.BindJobToNode(updatedNode, result.jctx.Job, result.jctx.Job.PriorityClass().Priority)
	if err != nil {
		return err
	}

	for i, n := range nodes {
		if n.GetId() == updatedNode.GetId() {
			nodes[i] = updatedNode
			break
		}
	}
	return nil
}

func (gp *GangPricer) groupNodesByNodeUniformityLabel(nodeUniformityLabel string, nodes []*internaltypes.Node) map[string][]*internaltypes.Node {
	if nodeUniformityLabel == "" {
		return map[string][]*internaltypes.Node{"default": nodes}
	}

	result := map[string][]*internaltypes.Node{}
	for _, node := range nodes {
		if value, ok := node.GetLabelValue(nodeUniformityLabel); ok {
			if _, present := result[value]; !present {
				result[value] = []*internaltypes.Node{}
			}
			result[value] = append(result[value], node)
		}
	}

	return result
}

func (gp *GangPricer) isValidNodeUniformityLabel(nodeUniformityLabel string) (bool, string) {
	if nodeUniformityLabel == "" {
		return true, ""
	}
	nodeUniformityLabelValues, ok := gp.nodeDb.IndexedNodeLabelValues(nodeUniformityLabel)
	if !ok {
		return false, GangUniformityLabelIsNotIndexedUnschedulableReason
	}
	if len(nodeUniformityLabelValues) == 0 {
		return false, GangNoNodesWithUniformityLabelUnschedulableReason
	}
	return true, ""
}

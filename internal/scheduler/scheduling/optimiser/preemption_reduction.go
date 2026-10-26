package optimiser

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
)

// This code aims to reduce preemptions on a provisional schedulingResult.
// - It achieves this by iterating over each preempted gang and attempting to keep it, via tryKeepGang
// - tryKeepGang works as follows:
//  - Preempts all preempted gangs, retaining only the gangsToKeep
//  - Determines which scheduled jobs have been displaced by retaining gangsToKeep
//  - Attempts to find free nodes for displaced jobs
//  - The above is done in a transaction, which is aborted on method return

// TODO: This code exposes some NodeDB limitations:
//  - Repeatedly preempting all gangsToPreempt for every gang we're trying to keep is inefficient, since
//    |gangsToKeep| << |gangsToPreempt| in the common case.
//  - Ideally we would: preempt all gangsToPreempt, iterate over preempted gangs and call tryKeepGang on each, then
//    finally restore any non-kept gangs. tryKeepGangs in this setup would only bind gangsToKeep to nodes.
//  - The existing NodeDB would require us to commit the preemption changes before trying to keep each gang.
//  - Mutating the NodeDB here is not desirable since it increases the risk of state changes leaking through.
//  - Ideally we would support (some form of) nested transaction within the NodeDB, which would make it easier to
//    run the sort of scheduling permutations we see below.

// reducePreemptions tries to keep each preempted gang on its original nodes, in
// preemption order. Only incoming jobs may move, and no further preemption is allowed.
func (n *FairnessOptimisingGangScheduler) reducePreemptions(ctx *armadacontext.Context, nodes []*internaltypes.Node, initialSchedulingResult *schedulingResult) (bestSchedulingResult *schedulingResult, err error) {
	bestSchedulingResult = initialSchedulingResult
	gangsToKeep := map[string]*preemptibleGang{}
	for _, placement := range initialSchedulingResult.nodeResults {
		for _, gang := range placement.gangsToPreempt {
			if ctx.Err() != nil {
				return nil, nil
			}

			var err error
			var retained bool
			gangsToKeep[gang.preemptedSiblingJobId] = gang
			retained, bestSchedulingResult, err = n.tryKeepGang(ctx, nodes, bestSchedulingResult, gangsToKeep)
			if err != nil {
				return nil, err
			}
			if !retained {
				delete(gangsToKeep, gang.preemptedSiblingJobId)
			}
		}
	}

	return bestSchedulingResult, nil
}

// tryKeepGang will attempt to retain gangsToKeep, shifting any jobs displaced from the incoming gang onto nodes
// provided. If we cannot keep the gang, initialSchedulingResult will be returned.
func (n *FairnessOptimisingGangScheduler) tryKeepGang(ctx *armadacontext.Context, nodes []*internaltypes.Node, initialSchedulingResult *schedulingResult, gangsToKeep map[string]*preemptibleGang) (bool, *schedulingResult, error) {
	txn := n.nodeDb.Txn(true)
	defer txn.Abort()

	reducedSchedulingResult := &schedulingResult{scheduled: true, nodeResults: make([]*nodeSchedulingResult, 0, len(initialSchedulingResult.nodeResults))}

	// Preempt all gangs except the ones to keep
	for _, nodeResult := range initialSchedulingResult.nodeResults {
		if ctx.Err() != nil {
			return false, nil, nil
		}
		candidate := *nodeResult
		candidate.gangsToPreempt = nil
		for _, gang := range nodeResult.gangsToPreempt {
			if _, ok := gangsToKeep[gang.preemptedSiblingJobId]; ok {
				continue
			}
			if err := n.unbindGangWithTxn(txn, gang); err != nil {
				return false, nil, err
			}
			candidate.gangsToPreempt = append(candidate.gangsToPreempt, gang)
		}
		reducedSchedulingResult.nodeResults = append(reducedSchedulingResult.nodeResults, &candidate)
	}

	// Check which incoming gang members have been displaced by keeping gangToKeep
	displaced := make([]*nodeSchedulingResult, 0)
	for _, placement := range reducedSchedulingResult.nodeResults {
		node, err := n.nodeDb.GetNodeWithTxn(txn, placement.node.GetId())
		if err != nil {
			return false, nil, err
		}
		if placement.jctx.KubernetesResourceRequirements.Exceeds(node.AllocatableAtPriority(internaltypes.EvictedPriority)) {
			placement.gangsToPreempt = []*preemptibleGang{}
			placement.node = nil
			placement.schedulingCost = 0
			placement.maximumQueueImpact = 0
			for q := range placement.queueCostChanges {
				placement.queueCostChanges[q] = 0
			}

			displaced = append(displaced, placement)
			continue
		}
		if err := n.bindJobWithTxn(txn, placement.jctx, node.GetId()); err != nil {
			return false, nil, err
		}
	}

	// Find alternative placements for displaced incoming gangs
	for _, toPlace := range displaced {
		placed := false
		for _, originalNode := range nodes {
			if ctx.Err() != nil {
				return false, nil, nil
			}
			node, err := n.nodeDb.GetNodeWithTxn(txn, originalNode.GetId())
			if err != nil {
				return false, nil, err
			}
			fits, _, err := nodedb.JobRequirementsMet(node, internaltypes.EvictedPriority, toPlace.jctx)
			if err != nil {
				return false, nil, err
			}
			if !fits {
				continue
			}
			if err := n.bindJobWithTxn(txn, toPlace.jctx, node.GetId()); err != nil {
				return false, nil, err
			}
			toPlace.node = node
			placed = true
			break
		}
		if !placed {
			return false, initialSchedulingResult, nil
		}
	}
	return true, reducedSchedulingResult, nil
}

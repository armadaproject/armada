package context

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"
)

type SchedulingType int

const (
	None SchedulingType = iota
	Rescheduled
	ScheduledWithoutPreemption
	ScheduledWithFairSharePreemption
	ScheduledWithUrgencyBasedPreemption
	ScheduledAsAwayJob
)

// PodSchedulingContext is returned by SelectAndBindNodeToPod and
// contains detailed information on the scheduling decision made for this pod.
type PodSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// ID of the node that the pod was assigned to, or empty.
	NodeId string
	// If set, indicates that the pod was scheduled on a specific node type.
	WellKnownNodeTypeName string
	// Priority this pod was most recently attempted to be scheduled at.
	// If scheduling was successful, resources were marked as allocated to the job at this priority.
	ScheduledAtPriority int32
	// Maximum priority that this pod preempted other pods at.
	PreemptedAtPriority int32
	// Total number of nodes in the cluster when trying to schedule.
	NumNodes int
	// Number of nodes excluded by reason.
	NumExcludedNodesByReason map[string]int
	// If this pod was scheduled as an away job
	ScheduledAway bool
	// The method of scheduling that was used to schedule this job
	SchedulingMethod SchedulingType
}

func (pctx *PodSchedulingContext) IsSuccessful() bool {
	return pctx != nil && pctx.NodeId != ""
}

func (pctx *PodSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	if pctx.NodeId != "" {
		fmt.Fprintf(w, "Node:\t%s\n", pctx.NodeId)
	} else {
		fmt.Fprint(w, "Node:\tnone\n")
	}
	fmt.Fprintf(w, "Number of nodes in cluster:\t%d\n", pctx.NumNodes)
	if len(pctx.NumExcludedNodesByReason) == 0 {
		fmt.Fprint(w, "Excluded nodes:\tnone\n")
	} else {
		fmt.Fprint(w, "Excluded nodes:\n")
		for reason, count := range pctx.NumExcludedNodesByReason {
			fmt.Fprintf(w, "\t%d:\t%s\n", count, reason)
		}
	}
	w.Flush()
	return sb.String()
}

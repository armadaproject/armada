package internaltypes

import v1 "k8s.io/api/core/v1"

const (
	unschedulableTaintKey    string         = "armadaproject.io/unschedulable"
	unschedulableTaintValue  string         = "true"
	unschedulableTaintEffect v1.TaintEffect = v1.TaintEffectNoSchedule
)

// UnschedulableTaint returns the taint automatically added to unschedulable nodes on inserting into the nodeDb.
func UnschedulableTaint() v1.Taint {
	return v1.Taint{
		Key:    unschedulableTaintKey,
		Value:  unschedulableTaintValue,
		Effect: unschedulableTaintEffect,
	}
}

// IsCordonTaint reports whether taint marks a node as cordoned, i.e. temporarily not accepting new
// jobs. This covers both the Armada-synthesized taint (added when a node reports unschedulable=true)
// and the taint Kubernetes adds automatically on `kubectl cordon` (v1.TaintNodeUnschedulable).
// A cordon is transient, so feasibility checks (e.g. the submit checker) should ignore these taints.
//
// Matching is by key only, not effect. Both cordon taints are always NoSchedule in practice, and for
// a feasibility check any taint under these keys is transient operational state we want to ignore, so
// we strip it regardless of effect rather than risk rejecting a job that could run once uncordoned.
func IsCordonTaint(taint v1.Taint) bool {
	return taint.Key == unschedulableTaintKey || taint.Key == v1.TaintNodeUnschedulable
}

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

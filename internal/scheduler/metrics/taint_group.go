package metrics

import "github.com/armadaproject/armada/internal/scheduler/internaltypes"

const (
	taintCategoryUnspecified = ""
	taintCategoryUser        = "user"
	taintCategoryUnknown     = "unknown"
	taintGroupTypeAdmin      = "admin"
	taintGroupTypeNone       = "none"
)

// classifyTaintGroup classifies reported taint metadata for node metrics.
// No metadata is reported as none, even when a node has raw taints.
func classifyTaintGroup(taints []internaltypes.NodeTaint) string {
	if len(taints) == 0 {
		return taintGroupTypeNone
	}

	unknownTaintsFound := false
	for _, taint := range taints {
		switch taint.GetCategory() {
		case taintCategoryUnspecified, taintCategoryUnknown:
			unknownTaintsFound = true
		case taintCategoryUser:
		default:
			return taintGroupTypeAdmin
		}
	}

	if unknownTaintsFound {
		return taintCategoryUnknown
	}

	return taintCategoryUser
}

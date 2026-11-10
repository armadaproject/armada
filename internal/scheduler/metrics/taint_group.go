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
// No metadata, or only Armada's synthetic unschedulable marker, is reported as none,
// even when a node has raw taints.
func classifyTaintGroup(taints []internaltypes.NodeTaint) string {
	hasTaints := false
	unknownTaintsFound := false
	unschedulableKey := internaltypes.UnschedulableTaint().Key
	for _, taint := range taints {
		if taint.GetTaint().Key == unschedulableKey {
			continue
		}
		hasTaints = true
		switch taint.GetCategory() {
		case taintCategoryUnspecified:
			// Legacy tolerated and reservation taints have no category but are user-facing.
			if !taint.GetUserFacing() {
				unknownTaintsFound = true
			}
		case taintCategoryUnknown:
			unknownTaintsFound = true
		case taintCategoryUser:
		default:
			// Any other non-empty category is administrator-defined.
			return taintGroupTypeAdmin
		}
	}

	if !hasTaints {
		return taintGroupTypeNone
	}
	if unknownTaintsFound {
		return taintCategoryUnknown
	}

	return taintCategoryUser
}

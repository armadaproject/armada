package metrics

import (
	"sort"
	"strings"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

const (
	taintCategoryUnspecified = ""
	taintCategoryUser        = "user"
	taintCategoryUnknown     = "unknown"
	taintGroupTypeNone       = "none"
)

// classifyTaintGroup classifies reported taint metadata for node metrics:
//   - If any admin taints exist, it returns their keys sorted and joined by "|".
//   - Otherwise, it returns "user" if any taint has the user category or is
//     user-facing with no category.
//   - Otherwise, it returns "none". Unknown taints are ignored.
func classifyTaintGroup(taints []internaltypes.NodeTaint) string {
	hasUserTaints := false
	var adminTaintKeys []string
	for _, taint := range taints {
		switch category := taint.GetCategory(); {
		case category == taintCategoryUser || (category == taintCategoryUnspecified && taint.GetUserFacing()):
			hasUserTaints = true
		case category != taintCategoryUnspecified && category != taintCategoryUnknown:
			// Any other non-empty category is administrator-defined.
			adminTaintKeys = append(adminTaintKeys, taint.GetTaint().Key)
		default:
			// Unknown taints do not contribute to the group type.
		}
	}

	if len(adminTaintKeys) > 0 {
		sort.Strings(adminTaintKeys)
		return strings.Join(adminTaintKeys, "|")
	}
	if hasUserTaints {
		return taintCategoryUser
	}

	return taintGroupTypeNone
}

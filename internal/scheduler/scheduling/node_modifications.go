package scheduling

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
)

// applyTaintModifications returns the taint set for a node after applying the given taint
// modifications. Deletes are applied before adds, regardless of list order.
//
// A Delete matcher removes any taint whose key matches and where:
//   - value == "*" matches any value, otherwise the value must match exactly;
//   - effect must always match exactly (there is no effect wildcard).
//
// An Add appends the taint verbatim, skipping only exact (key, value, effect) duplicates.
func applyTaintModifications(current []v1.Taint, mods []configuration.TaintModification) []v1.Taint {
	result := make([]v1.Taint, 0, len(current))

	// Deletes first.
	for _, taint := range current {
		if taintMatchesAnyDelete(taint, mods) {
			continue
		}
		result = append(result, taint)
	}

	// Then adds.
	for _, mod := range mods {
		if mod.Operation != configuration.TaintOperationAdd {
			continue
		}
		if containsExactTaint(result, mod.Taint) {
			continue
		}
		result = append(result, mod.Taint)
	}

	return result
}

func taintMatchesAnyDelete(taint v1.Taint, mods []configuration.TaintModification) bool {
	for _, mod := range mods {
		if mod.Operation != configuration.TaintOperationDelete {
			continue
		}
		m := mod.Taint
		if m.Key != taint.Key {
			continue
		}
		// Value "*" is a wildcard; otherwise the value must match exactly.
		if m.Value != configuration.WildCardWellKnownNodeTypeValue && m.Value != taint.Value {
			continue
		}
		// Effect must always match exactly; there is no effect wildcard.
		if m.Effect != taint.Effect {
			continue
		}
		return true
	}
	return false
}

func containsExactTaint(taints []v1.Taint, target v1.Taint) bool {
	for _, t := range taints {
		if t.Key == target.Key && t.Value == target.Value && t.Effect == target.Effect {
			return true
		}
	}
	return false
}

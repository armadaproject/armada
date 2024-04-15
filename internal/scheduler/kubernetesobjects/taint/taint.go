package taint

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"
)

func DeepCopyTaints(taints []v1.Taint) []v1.Taint {
	return DeepCopyTaintsInternStrings(taints, func(s string) string { return s })
}

func DeepCopyTaint(taint *v1.Taint) v1.Taint {
	return DeepCopyTaintInternStrings(taint, func(s string) string { return s })
}

func DeepCopyTaintsInternStrings(taints []v1.Taint, stringInterner func(string) string) []v1.Taint {
	result := make([]v1.Taint, len(taints))
	for i, taint := range taints {
		result[i] = DeepCopyTaintInternStrings(&taint, stringInterner)
	}
	return result
}

func DeepCopyTaintInternStrings(taint *v1.Taint, stringInterner func(string) string) v1.Taint {
	var pTimedAdded *metav1.Time = nil
	if taint.TimeAdded != nil {
		timedAdded := *taint.TimeAdded
		pTimedAdded = &timedAdded
	}

	return v1.Taint{
		Key:       stringInterner(taint.Key),
		Value:     stringInterner(taint.Value),
		Effect:    v1.TaintEffect(stringInterner(string(taint.Effect))),
		TimeAdded: pTimedAdded,
	}
}

// findMatchingUntoleratedTaint checks if the given tolerations tolerates
// all the taints, and returns the first taint without a toleration.
// Returns true if there is an untolerated taint.
// Returns false if all taints are tolerated.
func FindMatchingUntoleratedTaint(taints []v1.Taint, tolerations ...[]v1.Toleration) (v1.Taint, bool) {
	for _, taint := range taints {
		taintTolerated := false
		for _, ts := range tolerations {
			taintTolerated = taintTolerated || corev1.TolerationsTolerateTaint(ts, &taint)
			if taintTolerated {
				break
			}
		}
		if !taintTolerated {
			return DeepCopyTaint(&taint), true
		}
	}
	return v1.Taint{}, false
}

package affinity

import (
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

func AddNodeAntiAffinity(affinity *v1.Affinity, labelName string, labelValue string) error {
	if affinity == nil {
		return errors.Errorf("failed to add not anti affinity, as provided affinity is nil")
	}
	ensureAffinityHasNodeSelectorTerms(affinity)
	addAvoidNodeAffinityToNodeSelectorTerms(affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, labelName, labelValue)
	return nil
}

func ensureAffinityHasNodeSelectorTerms(affinity *v1.Affinity) {
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	nodeAffinity := affinity.NodeAffinity

	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}
	ns := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

	if len(ns.NodeSelectorTerms) == 0 {
		ns.NodeSelectorTerms = []v1.NodeSelectorTerm{{}}
	}
}

func addAvoidNodeAffinityToNodeSelectorTerms(terms []v1.NodeSelectorTerm, labelName string, labelValue string) {
	// The node NotIn needs to be added to all NodeSelectorTerms, because if any single NodeSelectorTerm matches
	// the pod is considered to match.
	for i := range terms {
		addAvoidNodeAffinityToNodeSelectorTerm(&terms[i], labelName, labelValue)
	}
}

func addAvoidNodeAffinityToNodeSelectorTerm(term *v1.NodeSelectorTerm, labelName string, labelValue string) {
	mexp := findMatchExpression(term.MatchExpressions, labelName, v1.NodeSelectorOpNotIn)
	if mexp == nil {
		term.MatchExpressions = append(term.MatchExpressions, v1.NodeSelectorRequirement{
			Key:      labelName,
			Operator: v1.NodeSelectorOpNotIn,
			Values:   []string{},
		})
		mexp = &term.MatchExpressions[len(term.MatchExpressions)-1]
	}

	if !slices.Contains(mexp.Values, labelValue) {
		mexp.Values = append(mexp.Values, labelValue)
	}
}

func findMatchExpression(matchExpressions []v1.NodeSelectorRequirement, key string, operator v1.NodeSelectorOperator) *v1.NodeSelectorRequirement {
	for i, me := range matchExpressions {
		if me.Key == key && me.Operator == operator {
			return &matchExpressions[i]
		}
	}
	return nil
}

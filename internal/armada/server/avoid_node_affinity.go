package server

import (
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

func addAvoidNodeAffinity(job *api.Job, avoidNodeLabels *api.OrderedStringMap, validateJobsCanBeScheduled func(jobs []*api.Job) error) bool {
	changed := false
	for _, label := range avoidNodeLabels.Entries {

		candidateNewJob := addAvoidNodeAffinityInner(job, label.Key, label.Value)

		err := validateJobsCanBeScheduled([]*api.Job{candidateNewJob})
		if err != nil {
			log.Warnf(
				"Not adding avoid node affinity for label %s=%s for job %s because doing so would mean the job would not match any nodes due to error: %s",
				label.Key,
				label.Value,
				job.Id,
				err,
			)
			continue
		}

		log.Infof("Adding avoid node affinity for label %s=%s for job %s", label.Key, label.Value, job.Id)
		*job = *candidateNewJob
		changed = true
	}
	return changed
}

func addAvoidNodeAffinityInner(job *api.Job, labelName string, labelValue string) *api.Job {
	result := proto.Clone(job).(*api.Job)

	addAvoidNodeAffinityToPod(result.PodSpec, labelName, labelValue)
	for _, pod := range result.PodSpecs {
		addAvoidNodeAffinityToPod(pod, labelName, labelValue)
	}

	return result
}

func addAvoidNodeAffinityToPod(pod *v1.PodSpec, labelName string, labelValue string) {
	ensurePodHasNodeSelectorTerms(pod)
	addAvoidNodeAffinityToNodeSelectorTerms(pod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, labelName, labelValue)
}

func ensurePodHasNodeSelectorTerms(pod *v1.PodSpec) {
	if pod.Affinity == nil {
		pod.Affinity = &v1.Affinity{}
	}
	affinity := pod.Affinity

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

	if !util.ContainsString(mexp.Values, labelValue) {
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

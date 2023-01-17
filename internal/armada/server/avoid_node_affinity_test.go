package server

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/api"
)

func Test_addAvoidNodeAffinity_WhenCanBeScheduled_AddsAffinities(t *testing.T) {
	labels := []*api.StringKeyValuePair{{Key: "name1", Value: "val1"}, {Key: "name2", Value: "val2"}}

	job := basicJob()
	changed := addAvoidNodeAffinity(job, &api.OrderedStringMap{Entries: labels}, func(jobs []*api.Job) error { return nil })

	expectedJob := basicJob()
	expectedJob.PodSpec.Affinity = vanillaAvoidLabelAffinites(labels)
	expectedJob.PodSpecs[0].Affinity = vanillaAvoidLabelAffinites(labels)

	assert.Equal(t, expectedJob, job)
	assert.True(t, changed)
}

func Test_addAvoidNodeAffinity_WhenCannotBeScheduled_DoesNotAddAffinities(t *testing.T) {
	labels := []*api.StringKeyValuePair{{Key: "name1", Value: "val1"}, {Key: "name2", Value: "val2"}}

	job := basicJob()
	changed := addAvoidNodeAffinity(job, &api.OrderedStringMap{Entries: labels}, func(jobs []*api.Job) error { return errors.New("Can't schedule") })

	expectedJob := basicJob()

	assert.Equal(t, expectedJob, job)
	assert.False(t, changed)
}

func Test_addAvoidNodeAffinity_WhenFirstAffinityCannotBeScheduled_ButSecondCan_AddsOnlySecond(t *testing.T) {
	labels := []*api.StringKeyValuePair{{Key: "name1", Value: "val1"}, {Key: "name2", Value: "val2"}}

	job := basicJob()
	changed := addAvoidNodeAffinity(job, &api.OrderedStringMap{Entries: labels}, func(jobs []*api.Job) error {
		if jobs[0].PodSpecs[0].Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Key == "name1" {
			return errors.New("Can't schedule")
		} else {
			return nil
		}
	})

	expectedJob := basicJob()
	expectedAffinity := vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{labels[1]})
	expectedJob.PodSpec.Affinity = expectedAffinity
	expectedJob.PodSpecs[0].Affinity = expectedAffinity

	assert.Equal(t, expectedJob, job)
	assert.True(t, changed)
}

func Test_addAvoidNodeAffinityToPod_WhenNoExistingAffinity_AddsCorrectly(t *testing.T) {
	pod := basicPod()
	addAvoidNodeAffinityToPod(pod, "a", "b")

	expectedPod := basicPod()
	expectedPod.Affinity = vanillaAvoidLabelAffinity("a", "b")

	assert.Equal(t, expectedPod, pod)
}

func Test_addAvoidNodeAffinityToPod_WhenAlreadyThere_DoesNothing(t *testing.T) {
	pod := basicPod()
	addAvoidNodeAffinityToPod(pod, "a", "b")

	expectedPod := pod.DeepCopy()
	addAvoidNodeAffinityToPod(pod, "a", "b")

	assert.Equal(t, expectedPod, pod)
}

func Test_addAvoidNodeAffinityToPod_WhenSameLabelDifferentValueAlreadyThere_IncludesBothValues(t *testing.T) {
	pod := basicPod()
	addAvoidNodeAffinityToPod(pod, "a", "b")
	addAvoidNodeAffinityToPod(pod, "a", "c")

	expectedPod := basicPod()
	expectedPod.Affinity = vanillaAvoidLabelAffinity("a", "b")
	expectedPod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"b", "c"}

	assert.Equal(t, expectedPod, pod)
}

func Test_addAvoidNodeAffinityToPod_WhenDifferentLabelAlreadyThere_IncludesBothLabels(t *testing.T) {
	pod := basicPod()
	addAvoidNodeAffinityToPod(pod, "a", "b")
	addAvoidNodeAffinityToPod(pod, "aa", "bb")

	expectedPod := basicPod()
	expectedPod.Affinity = vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{{Key: "a", Value: "b"}, {Key: "aa", Value: "bb"}})

	assert.Equal(t, expectedPod, pod)
}

func Test_addAvoidNodeAffinityToPod_WhenRandomJunkAlreadyThereThere_AddsLabelsLeavingJunkAlone(t *testing.T) {
	pod := basicPod()

	pod.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "junk1",
								Operator: v1.NodeSelectorOpGt,
								Values:   []string{"junk2"},
							},
						},
					},
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "junk3",
								Operator: v1.NodeSelectorOpExists,
								Values:   []string{"junk4"},
							},
						},
					},
				},
			},
		},
	}

	expectedPod := pod.DeepCopy()
	addLabelTerm := func(nst *v1.NodeSelectorTerm) {
		nst.MatchExpressions = append(nst.MatchExpressions, v1.NodeSelectorRequirement{
			Key:      "a",
			Operator: v1.NodeSelectorOpNotIn,
			Values:   []string{"b"},
		})
	}
	nsts := expectedPod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	addLabelTerm(&nsts[0])
	addLabelTerm(&nsts[1])

	addAvoidNodeAffinityToPod(pod, "a", "b")

	assert.Equal(t, expectedPod, pod)
}

func basicPod() *v1.PodSpec {
	return &v1.PodSpec{}
}

func basicJob() *api.Job {
	return &api.Job{PodSpecs: []*v1.PodSpec{basicPod()}, PodSpec: basicPod()}
}

func vanillaAvoidLabelAffinity(key string, val string) *v1.Affinity {
	return vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{{Key: key, Value: val}})
}

func vanillaAvoidLabelAffinites(labels []*api.StringKeyValuePair) *v1.Affinity {
	mexprs := []v1.NodeSelectorRequirement{}

	for _, kv := range labels {
		mexprs = append(mexprs, v1.NodeSelectorRequirement{
			Key:      kv.Key,
			Operator: v1.NodeSelectorOpNotIn,
			Values:   []string{kv.Value},
		})
	}

	return &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: mexprs,
					},
				},
			},
		},
	}
}

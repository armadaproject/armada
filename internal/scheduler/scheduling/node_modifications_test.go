package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
)

func TestCalculateTaints(t *testing.T) {
	noSchedule := v1.TaintEffectNoSchedule
	noExecute := v1.TaintEffectNoExecute

	tests := map[string]struct {
		current  []v1.Taint
		mods     []configuration.TaintModification
		expected []v1.Taint
	}{
		"nil mods is a no-op": {
			current:  []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
			mods:     nil,
			expected: []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
		},
		"add appends": {
			current:  []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationAdd, Taint: v1.Taint{Key: "b", Value: "2", Effect: noSchedule}}},
			expected: []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}, {Key: "b", Value: "2", Effect: noSchedule}},
		},
		"add skips exact duplicate": {
			current:  []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationAdd, Taint: v1.Taint{Key: "a", Value: "1", Effect: noSchedule}}},
			expected: []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
		},
		"add same key different effect appends": {
			current:  []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationAdd, Taint: v1.Taint{Key: "a", Value: "1", Effect: noExecute}}},
			expected: []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}, {Key: "a", Value: "1", Effect: noExecute}},
		},
		"delete exact match": {
			current:  []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}, {Key: "b", Value: "2", Effect: noSchedule}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationDelete, Taint: v1.Taint{Key: "a", Value: "1", Effect: noSchedule}}},
			expected: []v1.Taint{{Key: "b", Value: "2", Effect: noSchedule}},
		},
		"delete wildcard value matches any value with matching effect": {
			current:  []v1.Taint{{Key: "a", Value: "anything", Effect: noSchedule}, {Key: "b", Value: "2", Effect: noSchedule}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationDelete, Taint: v1.Taint{Key: "a", Value: "*", Effect: noSchedule}}},
			expected: []v1.Taint{{Key: "b", Value: "2", Effect: noSchedule}},
		},
		"delete effect must match exactly (no effect wildcard)": {
			current:  []v1.Taint{{Key: "a", Value: "x", Effect: noSchedule}, {Key: "a", Value: "y", Effect: noExecute}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationDelete, Taint: v1.Taint{Key: "a", Value: "*", Effect: noSchedule}}},
			expected: []v1.Taint{{Key: "a", Value: "y", Effect: noExecute}},
		},
		"delete exact value does not match a different value": {
			current:  []v1.Taint{{Key: "a", Value: "x", Effect: noSchedule}},
			mods:     []configuration.TaintModification{{Operation: configuration.TaintOperationDelete, Taint: v1.Taint{Key: "a", Value: "y", Effect: noSchedule}}},
			expected: []v1.Taint{{Key: "a", Value: "x", Effect: noSchedule}},
		},
		"delete before add regardless of list order": {
			current: []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
			mods: []configuration.TaintModification{
				{Operation: configuration.TaintOperationAdd, Taint: v1.Taint{Key: "a", Value: "1", Effect: noSchedule}},
				{Operation: configuration.TaintOperationDelete, Taint: v1.Taint{Key: "a", Value: "*", Effect: noSchedule}},
			},
			// Delete runs first (removes a), then Add re-adds a. Net result: a present.
			expected: []v1.Taint{{Key: "a", Value: "1", Effect: noSchedule}},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := applyTaintModifications(tc.current, tc.mods)
			assert.Equal(t, tc.expected, result)
		})
	}
}

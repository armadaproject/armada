package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReallocation(t *testing.T) {
	tests := map[string]struct {
		weights  []float64
		demand   []float64
		expected []float64
	}{
		"All equal weights": {
			weights:  []float64{1.0, 1.0, 1.0, 1.0},
			demand:   []float64{1.0, 1.0, 1.0, 1.0},
			expected: []float64{0.25, 0.25, 0.25, 0.25},
		},
		"One queue higher weight": {
			weights:  []float64{1.0, 2.0, 1.0, 1.0},
			demand:   []float64{1.0, 1.0, 1.0, 1.0},
			expected: []float64{0.2, 0.4, 0.2, 0.2},
		},
		"One queue has negligable demand": {
			weights:  []float64{1.0, 1.0, 1.0, 1.0},
			demand:   []float64{1.0, 1.0, 0.00001, 1.0},
			expected: []float64{0.33333, 0.33333, 0.00001, 0.33333},
		},
		"Balance after second iteration": {
			weights:  []float64{1.0, 1.0, 1.0, 1.0},
			demand:   []float64{1.0, 0.4, 0.00001, 0.00001},
			expected: []float64{0.59998, 0.4, 0.00001, 0.00001},
		},
		"All demand zeros": {
			weights:  []float64{1.0, 1.0, 1.0, 1.0},
			demand:   []float64{0.0, 0.0, 0.0, 0.0},
			expected: []float64{0.0, 0.0, 0.0, 0.0},
		},
		"All weights zeros": {
			weights:  []float64{0.0, 0.0, 0.0, 0.0},
			demand:   []float64{1.0, 1.0, 1.0, 1.0},
			expected: []float64{0.0, 0.0, 0.0, 0.0},
		},
		"High Demand": {
			weights:  []float64{1.0, 1.0, 1.0, 1.0},
			demand:   []float64{100.0, 99.0, 98.0, 97.0},
			expected: []float64{0.25, 0.25, 0.25, 0.25},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, calculateAdjustedShares(tc.weights, tc.demand, 5))
		})
	}
}

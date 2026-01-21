package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/constants"
)

func TestGetReservationName(t *testing.T) {
	tests := map[string]struct {
		input          []v1.Taint
		expectedResult string
	}{
		"reserved - single taint": {
			input:          []v1.Taint{{Key: constants.ReservationTaintKey, Value: "test", Effect: v1.TaintEffectNoSchedule}},
			expectedResult: "test",
		},
		"reserved - multiple taints": {
			input: []v1.Taint{
				{Key: "batch", Value: "true", Effect: v1.TaintEffectNoSchedule},
				{Key: constants.ReservationTaintKey, Value: "test", Effect: v1.TaintEffectNoSchedule},
				{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule},
			},
			expectedResult: "test",
		},
		"reserved - empty value": {
			input:          []v1.Taint{{Key: constants.ReservationTaintKey, Value: "", Effect: v1.TaintEffectNoSchedule}},
			expectedResult: EmptyReservationName,
		},
		"unreserved - no taints": {
			input:          []v1.Taint{},
			expectedResult: NoReservationName,
		},
		"unreserved - taints": {
			input: []v1.Taint{
				{Key: "batch", Value: "true", Effect: v1.TaintEffectNoSchedule},
				{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule},
			},
			expectedResult: NoReservationName,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := GetReservationName(tc.input)
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

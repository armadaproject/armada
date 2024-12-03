package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetProtectedFractionOfFairShare(t *testing.T) {
	zero := 0.0
	half := 0.5
	sc := SchedulingConfig{
		ProtectedFractionOfFairShare: 0.1,
		Pools: []PoolConfig{
			{
				Name:                         "overrides-pool",
				ProtectedFractionOfFairShare: &half,
			},
			{
				Name:                         "overrides-zero-pool",
				ProtectedFractionOfFairShare: &zero,
			},
			{
				Name: "not-set-pool",
			},
		},
	}

	assert.Equal(t, 0.5, sc.GetProtectedFractionOfFairShare("overrides-pool"))
	assert.Equal(t, 0.0, sc.GetProtectedFractionOfFairShare("overrides-zero-pool"))
	assert.Equal(t, 0.1, sc.GetProtectedFractionOfFairShare("not-set-pool"))
	assert.Equal(t, 0.1, sc.GetProtectedFractionOfFairShare("missing-pool"))
}

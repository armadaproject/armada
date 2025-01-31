package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

func compareQuantityMaps(a, b map[string]resource.Quantity) bool {
	if len(a) != len(b) {
		return false
	}
	for key, aVal := range a {
		bVal, exists := b[key]
		if !exists {
			return false
		}
		if aVal.String() != bVal.String() {
			return false
		}
	}
	return true
}

func TestUtilisationData_Aggregations(t *testing.T) {
	data := &UtilisationData{
		currentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		maxCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		sumCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		numDataPoints:   map[string]int64{"cpu": 1, "memory": 1},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("5")},
	}
	data2 := &UtilisationData{
		currentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		maxCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		sumCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		numDataPoints:   map[string]int64{"cpu": 1, "memory": 1},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
	}
	expected := &UtilisationData{
		currentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		maxCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("10")},
		sumCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("3"), "memory": resource.MustParse("11")},
		numDataPoints:   map[string]int64{"cpu": 2, "memory": 2},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
	}

	agg := data
	agg.Process(data2)

	assert.True(t, compareQuantityMaps(expected.currentUsage, agg.currentUsage))
	assert.True(t, compareQuantityMaps(expected.maxCurrentUsage, agg.maxCurrentUsage))
	assert.True(t, compareQuantityMaps(expected.sumCurrentUsage, agg.sumCurrentUsage))
	assert.Equal(t, expected.numDataPoints, agg.numDataPoints)
	assert.True(t, compareQuantityMaps(expected.cumulativeUsage, agg.cumulativeUsage))
}

func TestUtilisationData_GetAvg(t *testing.T) {
	data := &UtilisationData{
		currentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("10Mi")},
		maxCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("100Mi")},
		sumCurrentUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("160m"), "memory": resource.MustParse("110Mi")},
		numDataPoints:   map[string]int64{"cpu": 2, "memory": 2},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("160m")},
	}

	expected := armadaresource.ComputeResources{"cpu": resource.MustParse("80m"), "memory": resource.MustParse("55Mi")}

	assert.True(t, compareQuantityMaps(expected, data.GetAvgUsage()))
}

func TestUtilisationData_Max_WithEmpty(t *testing.T) {
	currentUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	maxCurrentUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	sumCurrentUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	numDataPoints := map[string]int64{"cpu": 1, "memory": 1}
	cumulativeUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("10")}
	data := &UtilisationData{
		currentUsage:    currentUsage.DeepCopy(),
		maxCurrentUsage: maxCurrentUsage.DeepCopy(),
		sumCurrentUsage: sumCurrentUsage.DeepCopy(),
		numDataPoints:   numDataPoints,
		cumulativeUsage: cumulativeUsage.DeepCopy(),
	}
	agg := EmptyUtilisationData()
	agg.Process(data)
	assert.Equal(t, data.currentUsage, agg.currentUsage)
	assert.Equal(t, data.maxCurrentUsage, agg.maxCurrentUsage)
	assert.Equal(t, data.sumCurrentUsage, agg.sumCurrentUsage)
	assert.Equal(t, data.numDataPoints, agg.numDataPoints)
	assert.Equal(t, data.cumulativeUsage, agg.cumulativeUsage)
}

func TestUtilisationData_IsEmpty(t *testing.T) {
	data := EmptyUtilisationData()
	assert.True(t, data.IsEmpty())

	cumulativeUsageNotEmpty := EmptyUtilisationData()
	cumulativeUsageNotEmpty.cumulativeUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, cumulativeUsageNotEmpty.IsEmpty())

	currentUsageNotEmpty := EmptyUtilisationData()
	currentUsageNotEmpty.currentUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, currentUsageNotEmpty.IsEmpty())

	allNotEmpty := EmptyUtilisationData()
	allNotEmpty.cumulativeUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	allNotEmpty.currentUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, allNotEmpty.IsEmpty())
}

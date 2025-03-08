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

func TestUtilisationDataAggregation(t *testing.T) {
	data := &UtilisationDataAggregation{
		maxUsage:        armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		sumOfUsage:      armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		numDataPoints:   map[string]int64{"cpu": 1, "memory": 1},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("5")},
	}
	data2 := &UtilisationData{
		CurrentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		CumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
	}
	expected := &UtilisationDataAggregation{
		maxUsage:        armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("10")},
		sumOfUsage:      armadaresource.ComputeResources{"cpu": resource.MustParse("3"), "memory": resource.MustParse("11")},
		numDataPoints:   map[string]int64{"cpu": 2, "memory": 2},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
	}

	aggregated := data
	aggregated.Add(data2)
	assert.True(t, compareQuantityMaps(expected.maxUsage, aggregated.maxUsage))
	assert.True(t, compareQuantityMaps(expected.sumOfUsage, aggregated.sumOfUsage))
	assert.Equal(t, expected.numDataPoints, aggregated.numDataPoints)
	assert.True(t, compareQuantityMaps(expected.cumulativeUsage, aggregated.cumulativeUsage))
}

func TestUtilisationDataAggregation_WithEmpty(t *testing.T) {
	currentUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	cumulativeUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("10")}
	data := &UtilisationData{
		CurrentUsage:    currentUsage.DeepCopy(),
		CumulativeUsage: cumulativeUsage.DeepCopy(),
	}
	aggregated := EmptyUtilisationDataAggregation()
	aggregated.Add(data)
	assert.Equal(t, data.CurrentUsage, aggregated.maxUsage)
	assert.Equal(t, data.CumulativeUsage, aggregated.cumulativeUsage)

	assert.True(t, compareQuantityMaps(data.CurrentUsage, aggregated.maxUsage))
	assert.True(t, compareQuantityMaps(data.CurrentUsage, aggregated.sumOfUsage))
	assert.True(t, compareQuantityMaps(data.CumulativeUsage, aggregated.cumulativeUsage))
}

func TestUtilisationData_GetAvg(t *testing.T) {
	data := &UtilisationDataAggregation{
		maxUsage:        armadaresource.ComputeResources{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("100Mi")},
		sumOfUsage:      armadaresource.ComputeResources{"cpu": resource.MustParse("160m"), "memory": resource.MustParse("110Mi")},
		numDataPoints:   map[string]int64{"cpu": 2, "memory": 2},
		cumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("160m")},
	}

	expected := armadaresource.ComputeResources{"cpu": resource.MustParse("80m"), "memory": resource.MustParse("55Mi")}

	assert.True(t, compareQuantityMaps(expected, data.GetAvgUsage()))
}

func TestUtilisationData_IsEmpty(t *testing.T) {
	data := EmptyUtilisationData()
	assert.True(t, data.IsEmpty())

	cumulativeUsageNotEmpty := EmptyUtilisationData()
	cumulativeUsageNotEmpty.CumulativeUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, cumulativeUsageNotEmpty.IsEmpty())

	currentUsageNotEmpty := EmptyUtilisationData()
	currentUsageNotEmpty.CurrentUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, currentUsageNotEmpty.IsEmpty())

	allNotEmpty := EmptyUtilisationData()
	allNotEmpty.CumulativeUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	allNotEmpty.CurrentUsage = armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, allNotEmpty.IsEmpty())
}

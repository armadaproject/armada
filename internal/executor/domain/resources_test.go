package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

func TestUtilisationData_Max(t *testing.T) {
	data := &UtilisationData{
		CurrentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		CumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("5")},
	}
	data2 := &UtilisationData{
		CurrentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		CumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
	}
	expected := &UtilisationData{
		CurrentUsage:    armadaresource.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("10")},
		CumulativeUsage: armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
	}

	max := data
	max.Max(data2)
	assert.Equal(t, expected.CurrentUsage, max.CurrentUsage)
	assert.Equal(t, expected.CumulativeUsage, max.CumulativeUsage)
}

func TestUtilisationData_Max_WithEmpty(t *testing.T) {
	currentUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	cumulativeUsage := armadaresource.ComputeResources{"cpu": resource.MustParse("10")}
	data := &UtilisationData{
		CurrentUsage:    currentUsage.DeepCopy(),
		CumulativeUsage: cumulativeUsage.DeepCopy(),
	}
	max := EmptyUtilisationData()
	max.Max(data)
	assert.Equal(t, data.CurrentUsage, max.CurrentUsage)
	assert.Equal(t, data.CumulativeUsage, max.CumulativeUsage)
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

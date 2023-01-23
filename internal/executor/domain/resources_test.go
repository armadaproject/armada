package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common"
)

func TestUtilisationData_Max(t *testing.T) {
	data := &UtilisationData{
		CurrentUsage:    common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10")},
		CumulativeUsage: common.ComputeResources{"cpu": resource.MustParse("5")},
	}
	data2 := &UtilisationData{
		CurrentUsage:    common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1")},
		CumulativeUsage: common.ComputeResources{"cpu": resource.MustParse("10")},
	}
	expected := &UtilisationData{
		CurrentUsage:    common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("10")},
		CumulativeUsage: common.ComputeResources{"cpu": resource.MustParse("10")},
	}

	max := data
	max.Max(data2)
	assert.Equal(t, expected.CurrentUsage, max.CurrentUsage)
	assert.Equal(t, expected.CumulativeUsage, max.CumulativeUsage)
}

func TestUtilisationData_Max_WithEmpty(t *testing.T) {
	currentUsage := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	cumulativeUsage := common.ComputeResources{"cpu": resource.MustParse("10")}
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
	cumulativeUsageNotEmpty.CumulativeUsage = common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, cumulativeUsageNotEmpty.IsEmpty())

	currentUsageNotEmpty := EmptyUtilisationData()
	currentUsageNotEmpty.CurrentUsage = common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, currentUsageNotEmpty.IsEmpty())

	allNotEmpty := EmptyUtilisationData()
	allNotEmpty.CumulativeUsage = common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	allNotEmpty.CurrentUsage = common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1")}
	assert.False(t, allNotEmpty.IsEmpty())
}

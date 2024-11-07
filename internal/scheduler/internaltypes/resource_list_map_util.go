package internaltypes

import (
	"strings"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func RlMapToString(m map[string]ResourceList) string {
	results := []string{}
	for k, v := range m {
		results = append(results, k+"="+v.String())
	}
	return strings.Join(results, " ")
}

func RlMapSumValues(m map[string]ResourceList) ResourceList {
	result := ResourceList{}
	for _, v := range m {
		result = result.Add(v)
	}
	return result
}

func RlMapAllZero(m map[string]ResourceList) bool {
	for _, v := range m {
		if !v.AllZero() {
			return false
		}
	}
	return true
}

func RlMapHasNegativeValues(m map[string]ResourceList) bool {
	for _, v := range m {
		if v.HasNegativeValues() {
			return true
		}
	}
	return false
}

func RlMapFromJobSchedulerObjects(m schedulerobjects.QuantityByTAndResourceType[string], rlFactory *ResourceListFactory) map[string]ResourceList {
	result := map[string]ResourceList{}
	for k, v := range m {
		result[k] = rlFactory.FromJobResourceListIgnoreUnknown(v.Resources)
	}
	return result
}

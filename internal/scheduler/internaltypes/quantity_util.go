package internaltypes

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

func QuantityToInt64RoundUp(q resource.Quantity, scale resource.Scale) int64 {
	return q.ScaledValue(scale)
}

func QuantityToInt64RoundDown(q resource.Quantity, scale resource.Scale) int64 {
	result := q.ScaledValue(scale)
	q2 := resource.NewScaledQuantity(result, scale)
	if q2.Cmp(q) > 0 {
		result--
	}
	return result
}

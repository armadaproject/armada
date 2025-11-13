package util

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/constants"
)

func GetReservationKey(taints []v1.Taint) string {
	for _, taint := range taints {
		if taint.Key == constants.ReservationTaintKey && taint.Value != "" {
			return taint.Value
		}
	}
	return ""
}

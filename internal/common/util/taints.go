package util

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/constants"
)

const (
	EmptyReservationName = "unspecified"
	NoReservationName    = "none"
)

func GetReservationName(taints []v1.Taint) string {
	for _, taint := range taints {
		if taint.Key == constants.ReservationTaintKey {
			if taint.Value == "" {
				return EmptyReservationName
			}
			return taint.Value
		}
	}
	return NoReservationName
}

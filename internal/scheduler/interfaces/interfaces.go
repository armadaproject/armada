package interfaces

import "github.com/armadaproject/armada/pkg/api"

type MinimalJob interface {
	Annotations() map[string]string
	PriorityClassName() string
	Gang() *api.Gang
}

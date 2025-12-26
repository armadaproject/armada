package internaltypes

import (
	"fmt"

	k8sResource "k8s.io/apimachinery/pkg/api/resource"
)

type ResourceType int

const (
	// A normal k8s resource, such as "memory" or "nvidia.com/gpu"
	Kubernetes ResourceType = iota
	// A floating resource that is not tied to a Kubernetes cluster or node,
	// e.g. "external-storage-connections".
	Floating = iota
)

type Resource struct {
	Name  string
	Value k8sResource.Quantity
	Scale k8sResource.Scale
	Type  ResourceType
}

func (r Resource) IsZero() bool {
	return r.Value.IsZero()
}

func (r Resource) IsNegative() bool {
	return r.Value.Sign() < 0
}

func (r Resource) String() string {
	return fmt.Sprintf("%s=%s", r.Name, r.Value.String())
}

package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
)

func TestResourceIsZero(t *testing.T) {
	tests := []struct {
		name     string
		resource Resource
		expected bool
	}{
		{
			name: "zero value",
			resource: Resource{
				Name:  "cpu",
				Value: *k8sResource.NewScaledQuantity(0, k8sResource.Milli),
				Scale: k8sResource.Milli,
				Type:  Kubernetes,
			},
			expected: true,
		},
		{
			name: "positive value",
			resource: Resource{
				Name:  "cpu",
				Value: *k8sResource.NewScaledQuantity(1000, k8sResource.Milli),
				Scale: k8sResource.Milli,
				Type:  Kubernetes,
			},
			expected: false,
		},
		{
			name: "negative value",
			resource: Resource{
				Name:  "cpu",
				Value: *k8sResource.NewScaledQuantity(-1000, k8sResource.Milli),
				Scale: k8sResource.Milli,
				Type:  Kubernetes,
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.resource.IsZero()
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestResourceIsNegative(t *testing.T) {
	tests := []struct {
		name     string
		resource Resource
		expected bool
	}{
		{
			name: "zero value",
			resource: Resource{
				Name:  "memory",
				Value: *k8sResource.NewScaledQuantity(0, k8sResource.Scale(0)),
				Scale: k8sResource.Scale(0),
				Type:  Kubernetes,
			},
			expected: false,
		},
		{
			name: "positive value",
			resource: Resource{
				Name:  "memory",
				Value: *k8sResource.NewScaledQuantity(1024*1024*1024, k8sResource.Scale(0)),
				Scale: k8sResource.Scale(0),
				Type:  Kubernetes,
			},
			expected: false,
		},
		{
			name: "negative raw value",
			resource: Resource{
				Name:  "memory",
				Value: *k8sResource.NewScaledQuantity(-1024*1024*1024, k8sResource.Scale(0)),
				Scale: k8sResource.Scale(0),
				Type:  Kubernetes,
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.resource.IsNegative()
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestResourceString(t *testing.T) {
	tests := []struct {
		name     string
		resource Resource
		expected string
	}{
		{
			name: "CPU with milli scale",
			resource: Resource{
				Name:  "cpu",
				Value: *k8sResource.NewScaledQuantity(1000, k8sResource.Milli),
				Scale: k8sResource.Milli,
				Type:  Kubernetes,
			},
			expected: "cpu=1",
		},
		{
			name: "Memory in bytes",
			resource: Resource{
				Name:  "memory",
				Value: *k8sResource.NewScaledQuantity(1024*1024*1024, k8sResource.Scale(0)),
				Scale: k8sResource.Scale(0),
				Type:  Kubernetes,
			},
			expected: "memory=1073741824",
		},
		{
			name: "Zero value",
			resource: Resource{
				Name:  "nvidia.com/gpu",
				Value: *k8sResource.NewScaledQuantity(0, k8sResource.Milli),
				Scale: k8sResource.Milli,
				Type:  Kubernetes,
			},
			expected: "nvidia.com/gpu=0",
		},
		{
			name: "Negative value",
			resource: Resource{
				Name:  "memory",
				Value: *k8sResource.NewScaledQuantity(-1024*1024, k8sResource.Scale(0)),
				Scale: k8sResource.Scale(0),
				Type:  Kubernetes,
			},
			expected: "memory=-1048576",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.resource.String()
			assert.Equal(t, test.expected, actual)
		})
	}
}

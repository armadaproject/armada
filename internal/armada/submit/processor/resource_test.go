package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

func TestResourceProcessor(t *testing.T) {
	defaultConfig := configuration.SchedulingConfig{
		DefaultJobLimits: map[string]resource.Quantity{
			"cpu":    resource.MustParse("10"),
			"memory": resource.MustParse("1Gi"),
		},
	}

	defaultExpected := map[v1.ResourceName]resource.Quantity{
		"cpu":    resource.MustParse("10"),
		"memory": resource.MustParse("1Gi"),
	}

	tests := map[string]struct {
		config   configuration.SchedulingConfig
		podSpec  v1.PodSpec
		expected v1.PodSpec
	}{
		"All Containers need defaults": {
			config: defaultConfig,
			podSpec: v1.PodSpec{
				Containers: []v1.Container{{}, {}},
			},
			expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: defaultExpected,
							Limits:   defaultExpected,
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: defaultExpected,
							Limits:   defaultExpected,
						},
					},
				},
			},
		},
		"One container needs defaults": {
			config: defaultConfig,
			podSpec: v1.PodSpec{
				Containers: []v1.Container{
					{},
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("20"),
								"memory": resource.MustParse("2Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("20"),
								"memory": resource.MustParse("2Gi"),
							},
						},
					},
				},
			},
			expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: defaultExpected,
							Limits:   defaultExpected,
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("20"),
								"memory": resource.MustParse("2Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("20"),
								"memory": resource.MustParse("2Gi"),
							},
						},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := resourceProcessor{
				defaultJobLimits: tc.config.DefaultJobLimits,
			}
			p.processPodSpec(&tc.podSpec)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

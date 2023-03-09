package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

func TestApplyDefaultsToPodSpec(t *testing.T) {
	tests := map[string]struct {
		Config   configuration.SchedulingConfig
		PodSpec  v1.PodSpec
		Expected v1.PodSpec
	}{
		"DefaultPriorityClassName": {
			Config: configuration.SchedulingConfig{
				Preemption: configuration.PreemptionConfig{
					Enabled:              true,
					DefaultPriorityClass: "pc",
				},
			},
			PodSpec: v1.PodSpec{},
			Expected: v1.PodSpec{
				PriorityClassName: "pc",
			},
		},
		"DefaultJobLimits": {
			Config: configuration.SchedulingConfig{
				DefaultJobLimits: map[string]resource.Quantity{
					"cpu":    resource.MustParse("10"),
					"memory": resource.MustParse("1Gi"),
				},
			},
			PodSpec: v1.PodSpec{
				Containers: []v1.Container{{}, {}},
			},
			Expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
		"DefaultJobTolerations": {
			Config: configuration.SchedulingConfig{
				DefaultJobTolerations: []v1.Toleration{{Key: "foo"}, {Key: "bar"}},
			},
			PodSpec: v1.PodSpec{
				Tolerations: []v1.Toleration{{Key: "baz"}},
			},
			Expected: v1.PodSpec{
				Tolerations: []v1.Toleration{{Key: "baz"}, {Key: "foo"}, {Key: "bar"}},
			},
		},
		"DefaultJobTolerationsByPriorityClass": {
			Config: configuration.SchedulingConfig{
				DefaultJobTolerationsByPriorityClass: map[string][]v1.Toleration{
					"pc-1": {{Key: "foo"}, {Key: "bar"}},
					"pc-2": {{Key: "oof"}, {Key: "rab"}},
				},
			},
			PodSpec: v1.PodSpec{
				PriorityClassName: "pc-1",
				Tolerations:       []v1.Toleration{{Key: "baz"}},
			},
			Expected: v1.PodSpec{
				PriorityClassName: "pc-1",
				Tolerations:       []v1.Toleration{{Key: "baz"}, {Key: "foo"}, {Key: "bar"}},
			},
		},
		"DefaultPriorityClassName + DefaultJobTolerationsByPriorityClass": {
			Config: configuration.SchedulingConfig{
				Preemption: configuration.PreemptionConfig{
					Enabled:              true,
					DefaultPriorityClass: "pc",
				},
				DefaultJobTolerationsByPriorityClass: map[string][]v1.Toleration{
					"pc": {{Key: "foo"}, {Key: "bar"}},
				},
			},
			PodSpec: v1.PodSpec{},
			Expected: v1.PodSpec{
				PriorityClassName: "pc",
				Tolerations:       []v1.Toleration{{Key: "foo"}, {Key: "bar"}},
			},
		},
		"DefaultActiveDeadlineSeconds": {
			Config: configuration.SchedulingConfig{
				DefaultActiveDeadline: time.Second,
			},
			Expected: v1.PodSpec{
				ActiveDeadlineSeconds: pointerFromValue(int64(1)),
			},
		},
		"DefaultActiveDeadlineSecondsByResource": {
			Config: configuration.SchedulingConfig{
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"memory": 2 * time.Minute,
					"gpu":    time.Minute,
				},
			},
			PodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
			},
			Expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				ActiveDeadlineSeconds: pointerFromValue(int64(120)),
			},
		},
		"DefaultActiveDeadlineSeconds + DefaultActiveDeadlineSecondsByResource": {
			Config: configuration.SchedulingConfig{
				DefaultActiveDeadline: time.Second,
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"gpu": time.Minute,
				},
			},
			PodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
			},
			Expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("10"),
								"memory": resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				ActiveDeadlineSeconds: pointerFromValue(int64(1)),
			},
		},
		"DefaultActiveDeadlineSecondsByResource trumps DefaultActiveDeadlineSeconds": {
			Config: configuration.SchedulingConfig{
				DefaultActiveDeadline: time.Minute,
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"gpu": time.Second,
				},
			},
			PodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"gpu": resource.MustParse("1"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
			},
			Expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"gpu": resource.MustParse("1"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				ActiveDeadlineSeconds: pointerFromValue(int64(1)),
			},
		},
		"DefaultActiveDeadlineSecondsByResource explicit zero resource": {
			Config: configuration.SchedulingConfig{
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"gpu": time.Second,
				},
			},
			PodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"gpu": resource.MustParse("0"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
			},
			Expected: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"gpu": resource.MustParse("0"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
			},
		},
		"MinTerminationGracePeriod": {
			Config: configuration.SchedulingConfig{
				MinTerminationGracePeriod: time.Second,
			},
			Expected: v1.PodSpec{
				TerminationGracePeriodSeconds: pointerFromValue(int64(1)),
			},
		},
		"MinTerminationGracePeriod convert 0 to 1": {
			Config: configuration.SchedulingConfig{
				MinTerminationGracePeriod: time.Second,
			},
			PodSpec: v1.PodSpec{
				TerminationGracePeriodSeconds: pointerFromValue(int64(0)),
			},
			Expected: v1.PodSpec{
				TerminationGracePeriodSeconds: pointerFromValue(int64(1)),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			applyDefaultsToPodSpec(&tc.PodSpec, tc.Config)
			assert.Equal(t, tc.Expected, tc.PodSpec)
		})
	}
}

func pointerFromValue[T any](v T) *T {
	return &v
}

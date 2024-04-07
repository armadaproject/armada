package defaults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestActiveDeadlineSecondsProcessor(t *testing.T) {
	tests := map[string]struct {
		config   configuration.SubmissionConfig
		podSpec  *v1.PodSpec
		expected *v1.PodSpec
	}{
		"DefaultActiveDeadlineSeconds": {
			config: configuration.SubmissionConfig{
				DefaultActiveDeadline: time.Second,
			},
			podSpec: &v1.PodSpec{},
			expected: &v1.PodSpec{
				ActiveDeadlineSeconds: pointer.Int64Ptr(1),
			},
		},
		"DefaultActiveDeadlineSecondsByResource": {
			config: configuration.SubmissionConfig{
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"memory": 2 * time.Minute,
					"gpu":    time.Minute,
				},
			},
			podSpec: &v1.PodSpec{
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
			expected: &v1.PodSpec{
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
				ActiveDeadlineSeconds: pointer.Int64Ptr(120),
			},
		},
		"DefaultActiveDeadlineSeconds + DefaultActiveDeadlineSecondsByResource": {
			config: configuration.SubmissionConfig{
				DefaultActiveDeadline: time.Second,
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"gpu": time.Minute,
				},
			},
			podSpec: &v1.PodSpec{
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
			expected: &v1.PodSpec{
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
				ActiveDeadlineSeconds: pointer.Int64Ptr(1),
			},
		},
		"DefaultActiveDeadlineSecondsByResource trumps DefaultActiveDeadlineSeconds": {
			config: configuration.SubmissionConfig{
				DefaultActiveDeadline: time.Minute,
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"gpu": time.Second,
				},
			},
			podSpec: &v1.PodSpec{
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
			expected: &v1.PodSpec{
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
				ActiveDeadlineSeconds: pointer.Int64Ptr(1),
			},
		},
		"DefaultActiveDeadlineSecondsByResource explicit zero resource": {
			config: configuration.SubmissionConfig{
				DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
					"gpu": time.Second,
				},
			},
			podSpec: &v1.PodSpec{
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
			expected: &v1.PodSpec{
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
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := activeDeadlineSecondsProcessor{
				defaultActiveDeadline:                  tc.config.DefaultActiveDeadline,
				defaultActiveDeadlineByResourceRequest: tc.config.DefaultActiveDeadlineByResourceRequest,
			}
			p.Apply(submitMsgFromPodSpec(tc.podSpec))
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

func submitMsgFromPodSpec(p *v1.PodSpec) *armadaevents.SubmitJob {
	return &armadaevents.SubmitJob{
		MainObject: &armadaevents.KubernetesMainObject{
			Object: &armadaevents.KubernetesMainObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: p,
				},
			},
		},
	}
}

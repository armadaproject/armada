package conversion

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestTemplateProcessor(t *testing.T) {
	jobId := util.NewULID()

	tests := map[string]struct {
		input    *armadaevents.SubmitJob
		expected *armadaevents.SubmitJob
	}{
		"Test Template Annotations": {
			input: &armadaevents.SubmitJob{
				JobId: jobId,
				ObjectMeta: &armadaevents.ObjectMeta{
					Annotations: map[string]string{
						"foo": "http://foo.com/{{JobId}}",
						"bar": "http://foo.com/{JobId}",
						"baz": "http://foo.com",
					},
				},
			},
			expected: &armadaevents.SubmitJob{
				JobId: jobId,
				ObjectMeta: &armadaevents.ObjectMeta{
					Annotations: map[string]string{
						"foo": "http://foo.com/JobId",
						"bar": fmt.Sprintf("http://foo.com/%s", jobId),
						"baz": "http://foo.com",
					},
				},
			},
		},
		"Test Template Labels": {
			input: &armadaevents.SubmitJob{
				JobId: jobId,
				ObjectMeta: &armadaevents.ObjectMeta{
					Labels: map[string]string{
						"foo": "http://foo.com/{{JobId}}",
						"bar": "http://foo.com/{JobId}",
						"baz": "http://foo.com",
					},
				},
			},
			expected: &armadaevents.SubmitJob{
				JobId: jobId,
				ObjectMeta: &armadaevents.ObjectMeta{
					Labels: map[string]string{
						"foo": "http://foo.com/JobId",
						"bar": fmt.Sprintf("http://foo.com/%s", jobId),
						"baz": "http://foo.com",
					},
				},
			},
		},
		"Test Template Nothing": {
			input: &armadaevents.SubmitJob{
				JobId: jobId,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Annotations: map[string]string{
							"baz": "http://foo.com",
						},
						Labels: map[string]string{
							"baz": "http://bar.com",
						},
					},
				},
			},
			expected: &armadaevents.SubmitJob{
				JobId: jobId,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Annotations: map[string]string{
							"baz": "http://foo.com",
						},
						Labels: map[string]string{
							"baz": "http://bar.com",
						},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			templateMeta(tc.input, configuration.SubmissionConfig{})
			assert.Equal(t, tc.expected, tc.input)
		})
	}
}

func TestDefaultGang(t *testing.T) {
	tests := map[string]struct {
		config      configuration.SubmissionConfig
		annotations map[string]string
		expected    map[string]string
	}{
		"no change": {
			annotations: make(map[string]string),
			expected:    make(map[string]string),
		},
		"No change for non-gang jobs": {
			config: configuration.SubmissionConfig{
				DefaultGangNodeUniformityLabel: "foo",
			},
			annotations: make(map[string]string),
			expected:    make(map[string]string),
		},
		"Empty default": {
			annotations: map[string]string{
				configuration.GangIdAnnotation: "bar",
			},
			expected: map[string]string{
				configuration.GangIdAnnotation:                  "bar",
				configuration.GangNodeUniformityLabelAnnotation: "",
			},
		},
		"Add when missing": {
			config: configuration.SubmissionConfig{
				DefaultGangNodeUniformityLabel: "foo",
			},
			annotations: map[string]string{
				configuration.GangIdAnnotation: "bar",
			},
			expected: map[string]string{
				configuration.GangIdAnnotation:                  "bar",
				configuration.GangNodeUniformityLabelAnnotation: "foo",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			submitMsg := submitMsgFromAnnotations(tc.annotations)
			defaultGangNodeUniformityLabel(submitMsg, tc.config)
			assert.Equal(t, submitMsgFromAnnotations(tc.expected), submitMsg)
		})
	}
}

func TestDefaultActiveDeadlineSeconds(t *testing.T) {
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
			defaultActiveDeadlineSeconds(tc.podSpec, tc.config)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

func TestDefaultTolerations(t *testing.T) {
	tests := map[string]struct {
		config   configuration.SubmissionConfig
		podSpec  *v1.PodSpec
		expected *v1.PodSpec
	}{
		"DefaultJobTolerations": {
			config: configuration.SubmissionConfig{
				DefaultJobTolerations: []v1.Toleration{{Key: "foo"}, {Key: "bar"}},
			},
			podSpec: &v1.PodSpec{
				Tolerations: []v1.Toleration{{Key: "baz"}},
			},
			expected: &v1.PodSpec{
				Tolerations: []v1.Toleration{{Key: "baz"}, {Key: "foo"}, {Key: "bar"}},
			},
		},
		"DefaultJobTolerationsByPriorityClass": {
			config: configuration.SubmissionConfig{
				DefaultJobTolerationsByPriorityClass: map[string][]v1.Toleration{
					"pc-1": {{Key: "foo"}, {Key: "bar"}},
					"pc-2": {{Key: "oof"}, {Key: "rab"}},
				},
			},
			podSpec: &v1.PodSpec{
				PriorityClassName: "pc-1",
				Tolerations:       []v1.Toleration{{Key: "baz"}},
			},
			expected: &v1.PodSpec{
				PriorityClassName: "pc-1",
				Tolerations:       []v1.Toleration{{Key: "baz"}, {Key: "foo"}, {Key: "bar"}},
			},
		},
		"DefaultJobTolerationsByResourceRequest": {
			config: configuration.SubmissionConfig{
				DefaultJobTolerationsByResourceRequest: map[string][]v1.Toleration{
					"gpu": {{Key: "foo"}, {Key: "bar"}},
				},
			},
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu": resource.MustParse("10"),
								"gpu": resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				Tolerations: []v1.Toleration{{Key: "baz"}},
			},
			expected: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu": resource.MustParse("10"),
								"gpu": resource.MustParse("1Gi"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				Tolerations: []v1.Toleration{{Key: "baz"}, {Key: "foo"}, {Key: "bar"}},
			},
		},
		"DefaultJobTolerationsByResourceRequest explicit zero resource": {
			config: configuration.SubmissionConfig{
				DefaultJobTolerationsByResourceRequest: map[string][]v1.Toleration{
					"gpu": {{Key: "foo"}, {Key: "bar"}},
				},
			},
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu": resource.MustParse("10"),
								"gpu": resource.MustParse("0"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				Tolerations: []v1.Toleration{{Key: "baz"}},
			},
			expected: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu": resource.MustParse("10"),
								"gpu": resource.MustParse("0"),
							},
							Limits: map[v1.ResourceName]resource.Quantity{},
						},
					},
				},
				Tolerations: []v1.Toleration{{Key: "baz"}},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			defaultTolerations(tc.podSpec, tc.config)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

func TestDefaultPriorityClass(t *testing.T) {
	tests := map[string]struct {
		config   configuration.SubmissionConfig
		podSpec  *v1.PodSpec
		expected *v1.PodSpec
	}{
		"Default PriorityClassName When Not Specified": {
			config: configuration.SubmissionConfig{
				DefaultPriorityClassName: "pc",
			},
			podSpec: &v1.PodSpec{},
			expected: &v1.PodSpec{
				PriorityClassName: "pc",
			},
		},
		"Don't Default PriorityClassName When Already Present": {
			config: configuration.SubmissionConfig{
				DefaultPriorityClassName: "pc",
			},
			podSpec: &v1.PodSpec{
				PriorityClassName: "pc2",
			},
			expected: &v1.PodSpec{
				PriorityClassName: "pc2",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			defaultPriorityClass(tc.podSpec, tc.config)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

func TestDefaultResource(t *testing.T) {
	defaultConfig := configuration.SubmissionConfig{
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
		config   configuration.SubmissionConfig
		podSpec  *v1.PodSpec
		expected *v1.PodSpec
	}{
		"All Containers need defaults": {
			config: defaultConfig,
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{{}, {}},
			},
			expected: &v1.PodSpec{
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
			podSpec: &v1.PodSpec{
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
			expected: &v1.PodSpec{
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
			defaultResource(tc.podSpec, tc.config)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

func TestDefaultTerminationGracePeriod(t *testing.T) {
	defaultConfig := configuration.SubmissionConfig{
		MinTerminationGracePeriod: 1 * time.Hour,
	}

	tests := map[string]struct {
		config   configuration.SubmissionConfig
		podSpec  *v1.PodSpec
		expected *v1.PodSpec
	}{
		"Don't Default When Specified": {
			config: defaultConfig,
			podSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(500),
			},
			expected: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(500),
			},
		},
		"Default When Missing": {
			config:  defaultConfig,
			podSpec: &v1.PodSpec{},
			expected: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(3600),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			defaultTerminationGracePeriod(tc.podSpec, tc.config)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}

func submitMsgFromAnnotations(annotations map[string]string) *armadaevents.SubmitJob {
	return &armadaevents.SubmitJob{
		ObjectMeta: &armadaevents.ObjectMeta{
			Annotations: annotations,
		},
	}
}

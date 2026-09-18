package conversion

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/constants"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
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

func TestDefaultGangNodeUniformity(t *testing.T) {
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
		"No change for non-gang jobs with some gang annotations": {
			config: configuration.SubmissionConfig{
				DefaultGangNodeUniformityLabel: "foo",
			},
			annotations: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "1",
			},
			expected: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "1",
			},
		},
		"Empty default": {
			annotations: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "2",
			},
			expected: map[string]string{
				constants.GangIdAnnotation:                  "bar",
				constants.GangCardinalityAnnotation:         "2",
				constants.GangNodeUniformityLabelAnnotation: "",
			},
		},
		"Add when missing": {
			config: configuration.SubmissionConfig{
				DefaultGangNodeUniformityLabel: "foo",
			},
			annotations: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "2",
			},
			expected: map[string]string{
				constants.GangIdAnnotation:                  "bar",
				constants.GangCardinalityAnnotation:         "2",
				constants.GangNodeUniformityLabelAnnotation: "foo",
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

func TestDefaultGangFailFastFlag(t *testing.T) {
	tests := map[string]struct {
		config      configuration.SubmissionConfig
		annotations map[string]string
		expected    map[string]string
	}{
		"No change for non-gang jobs": {
			annotations: make(map[string]string),
			expected:    make(map[string]string),
		},
		"No change for non-gang jobs with some gang annotations": {
			annotations: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "1",
			},
			expected: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "1",
			},
		},
		"Don't mutate existing": {
			annotations: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "2",
				constants.FailFastAnnotation:        "false",
			},
			expected: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "2",
				constants.FailFastAnnotation:        "false",
			},
		},
		"Add when missing": {
			annotations: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "2",
			},
			expected: map[string]string{
				constants.GangIdAnnotation:          "bar",
				constants.GangCardinalityAnnotation: "2",
				constants.FailFastAnnotation:        "true",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			submitMsg := submitMsgFromAnnotations(tc.annotations)
			defaultGangFailFastFlag(submitMsg, tc.config)
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
				InitContainers: []v1.Container{{}},
				Containers:     []v1.Container{{}, {}},
			},
			expected: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: defaultExpected,
							Limits:   defaultExpected,
						},
					},
				},
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
		"Main and init containers needs defaults": {
			config: defaultConfig,
			podSpec: &v1.PodSpec{
				InitContainers: []v1.Container{
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
				InitContainers: []v1.Container{
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

func TestAddGangIdLabel(t *testing.T) {
	tests := map[string]struct {
		annotations    map[string]string
		initialLabels  map[string]string
		expectedLabels map[string]string
		enabled        bool
	}{
		"Unchanged if no gang id set": {
			annotations: map[string]string{},
			enabled:     true,
		},
		"Label added if gang id set": {
			annotations: map[string]string{
				constants.GangIdAnnotation: "foo",
			},
			expectedLabels: map[string]string{
				constants.GangIdAnnotation: "foo",
			},
			enabled: true,
		},
		"Doesn't modify existing labels": {
			annotations: map[string]string{
				constants.GangIdAnnotation: "foo",
			},
			initialLabels: map[string]string{
				"fish": "chips",
			},
			expectedLabels: map[string]string{
				"fish":                     "chips",
				constants.GangIdAnnotation: "foo",
			},
			enabled: true,
		},
		"Unchanged if disabled": {
			annotations: map[string]string{
				constants.GangIdAnnotation: "foo",
			},
			enabled: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			submitMsg := submitMsgFromAnnotations(tc.annotations)
			submitMsg.ObjectMeta.Labels = tc.initialLabels
			addGangIdLabel(submitMsg, configuration.SubmissionConfig{AddGangIdLabel: tc.enabled})
			assert.Equal(t, tc.expectedLabels, submitMsg.ObjectMeta.Labels)
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

func TestDropPodLevelResourcesIfDisabled(t *testing.T) {
	podLevel := &v1.ResourceRequirements{
		Requests: v1.ResourceList{"cpu": resource.MustParse("2")},
		Limits:   v1.ResourceList{"cpu": resource.MustParse("2")},
	}

	tests := map[string]struct {
		initialResources  *v1.ResourceRequirements
		enabled           bool
		expectedResources *v1.ResourceRequirements
	}{
		"disabled clears the pod-level block": {
			initialResources: podLevel,
			enabled:          false,
		},
		"enabled preserves the pod-level block": {
			initialResources:  podLevel,
			enabled:           true,
			expectedResources: podLevel,
		},
		"unset pod-level block is left unset": {
			enabled: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			spec := &v1.PodSpec{Resources: tc.initialResources.DeepCopy()}
			dropPodLevelResourcesIfDisabled(spec, configuration.SubmissionConfig{PodLevelResources: tc.enabled})
			assert.Equal(t, tc.expectedResources, spec.Resources)
		})
	}
}

// A resource carried by the pod-level block must not be defaulted into the containers, or each
// container gets its own ceiling nested inside the pod's and the pooled budget is unusable.
func TestDefaultResourcePodLevel(t *testing.T) {
	defaults := configuration.SubmissionConfig{
		DefaultJobLimits: armadaresource.ComputeResources{
			"cpu":               resource.MustParse("1"),
			"memory":            resource.MustParse("1Gi"),
			"ephemeral-storage": resource.MustParse("8Gi"),
		},
	}
	rr := func(rl v1.ResourceList) *v1.ResourceRequirements {
		return &v1.ResourceRequirements{Requests: rl, Limits: rl}
	}
	cpuMem := v1.ResourceList{"cpu": resource.MustParse("6"), "memory": resource.MustParse("24Gi")}

	tests := map[string]struct {
		podLevel *v1.ResourceRequirements
		spec     *v1.PodSpec
		// absent lists resources that must not appear on any container, present the
		// container-level values that must.
		absent  []v1.ResourceName
		present v1.ResourceList
	}{
		"pooled resources are not defaulted into containers": {
			podLevel: rr(cpuMem),
			spec:     &v1.PodSpec{Containers: []v1.Container{{Name: "model"}, {Name: "solver"}}},
			absent:   []v1.ResourceName{"cpu", "memory"},
			// KEP-2837 cannot carry ephemeral-storage at the pod level, so it still defaults.
			present: v1.ResourceList{"ephemeral-storage": resource.MustParse("8Gi")},
		},
		"a resource the pod-level block omits still defaults": {
			podLevel: rr(v1.ResourceList{"memory": resource.MustParse("24Gi")}),
			spec:     &v1.PodSpec{Containers: []v1.Container{{Name: "only"}}},
			absent:   []v1.ResourceName{"memory"},
			present:  v1.ResourceList{"cpu": resource.MustParse("1")},
		},
		"a bare init container no longer gets whole-core cpu": {
			podLevel: rr(cpuMem),
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{{Name: "init"}},
				Containers:     []v1.Container{{Name: "main"}},
			},
			absent: []v1.ResourceName{"cpu", "memory"},
		},
		"no pod-level block defaults exactly as before": {
			spec:    &v1.PodSpec{Containers: []v1.Container{{Name: "c"}}},
			present: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.spec.Resources = tc.podLevel
			defaultResource(tc.spec, defaults)

			for _, c := range append(tc.spec.Containers, tc.spec.InitContainers...) {
				for _, rn := range tc.absent {
					assert.NotContains(t, c.Resources.Requests, rn, c.Name)
					assert.NotContains(t, c.Resources.Limits, rn, c.Name)
				}
				for rn, want := range tc.present {
					assert.Equal(t, want, c.Resources.Requests[rn], "%s requests %s", c.Name, rn)
					assert.Equal(t, want, c.Resources.Limits[rn], "%s limits %s", c.Name, rn)
				}
			}
			// The block itself is never modified.
			assert.Equal(t, tc.podLevel, tc.spec.Resources)
		})
	}
}

// The effective request stays the pod-level budget once defaulting no longer inflates the
// container sum.
func TestDefaultResourcePodLevelEffectiveRequest(t *testing.T) {
	spec := &v1.PodSpec{
		Resources: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("6"), "memory": resource.MustParse("24Gi")},
			Limits:   v1.ResourceList{"cpu": resource.MustParse("6"), "memory": resource.MustParse("24Gi")},
		},
		Containers: []v1.Container{{Name: "model"}, {Name: "solver"}},
	}
	defaultResource(spec, configuration.SubmissionConfig{
		DefaultJobLimits: armadaresource.ComputeResources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
	})

	effective := api.SchedulingResourceRequirementsFromPodSpec(spec).Requests
	assert.Equal(t, resource.MustParse("6"), effective["cpu"])
	assert.Equal(t, resource.MustParse("24Gi"), effective["memory"])
}

package validation

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/configuration"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
)

func TestValidateHasPodSpec(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"single podspec in podspec field": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{},
			},
			expectSuccess: true,
		},
		"single podspec in podspecs field": {
			req: &api.JobSubmitRequestItem{
				PodSpecs: []*v1.PodSpec{{}},
			},
			expectSuccess: true,
		},
		"multiple podspecs in podspecs field": {
			req: &api.JobSubmitRequestItem{
				PodSpecs: []*v1.PodSpec{{}, {}},
			},
			expectSuccess: false,
		},
		"podspecs and podspec": {
			req: &api.JobSubmitRequestItem{
				PodSpec:  &v1.PodSpec{},
				PodSpecs: []*v1.PodSpec{{}},
			},
			expectSuccess: false,
		},
		"no podspec": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateHasPodSpec(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateAffinity(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"No affinity": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{},
			},
			expectSuccess: true,
		},
		"valid affinity": {
			req: &api.JobSubmitRequestItem{
				PodSpec: podSpecFromNodeSelector(v1.NodeSelectorRequirement{
					Key:      "bar",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"bar"},
				}),
			},
			expectSuccess: true,
		},
		"invalid affinity": {
			req: &api.JobSubmitRequestItem{
				PodSpec: podSpecFromNodeSelector(v1.NodeSelectorRequirement{
					Key:      "/keys_cant_start_with_a_slash",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"bar"},
				}),
			},
			expectSuccess: false,
		},
		"PreferredDuringSchedulingIgnoredDuringExecution not allowed": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
								{},
							},
						},
					},
				},
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateAffinity(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateGangs(t *testing.T) {
	tests := map[string]struct {
		jobRequests   []*api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no gang jobs": {
			jobRequests:   []*api.JobSubmitRequestItem{{}, {}},
			expectSuccess: true,
		},
		"complete gang job of cardinality 1 with no minimum cardinality provided": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			expectSuccess: true,
		},
		"empty gangId": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "",
						configuration.GangCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			expectSuccess: false,
		},
		"complete gang job of cardinality 3": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
			},
			expectSuccess: true,
		},
		"two complete gangs": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
			},
			expectSuccess: true,
		},
		"one complete and one incomplete gang are passed through": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
			},
			expectSuccess: true,
		},
		"missing cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation: "bar",
					},
				},
			},
			expectSuccess: false,
		},
		"invalid cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "not an int",
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation: "not an int",
					},
				},
			},
			expectSuccess: false,
		},
		"zero cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "0",
					},
				},
			},
			expectSuccess: false,
		},
		"negative cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "-1",
					},
				},
			},
			expectSuccess: false,
		},
		"inconsistent cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
			},
			expectSuccess: false,
		},
		"inconsistent PriorityClassName": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
					PodSpec: &v1.PodSpec{
						PriorityClassName: "baz",
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
					PodSpec: &v1.PodSpec{
						PriorityClassName: "zab",
					},
				},
			},
			expectSuccess: false,
		},
		"inconsistent NodeUniformityLabel": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:                  "bar",
						configuration.GangCardinalityAnnotation:         strconv.Itoa(2),
						configuration.GangNodeUniformityLabelAnnotation: "foo",
					},
					PodSpec: &v1.PodSpec{},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:                  "bar",
						configuration.GangCardinalityAnnotation:         strconv.Itoa(2),
						configuration.GangNodeUniformityLabelAnnotation: "bar",
					},
					PodSpec: &v1.PodSpec{},
				},
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateGangs(
				&api.JobSubmitRequest{JobRequestItems: tc.jobRequests},
				configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateIngresses(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no ingress": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: true,
		},
		"valid ingress": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
				},
			},
			expectSuccess: true,
		},
		"multiple ingress": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							6,
						},
					},
				},
			},
			expectSuccess: true,
		},
		"multiple ports": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5, 6,
						},
					},
				},
			},
			expectSuccess: true,
		},
		"no ports": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type:  api.IngressType_Ingress,
						Ports: []uint32{},
					},
				},
			},
			expectSuccess: false,
		},
		"duplicate ports": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5, 6, 5,
						},
					},
				},
			},
			expectSuccess: false,
		},
		"duplicate ports on different ingresses": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
				},
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateIngresses(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateNamespace(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"good namespace": {
			req: &api.JobSubmitRequestItem{
				Namespace: "my-namespace",
			},
			expectSuccess: true,
		},
		"missing namespace": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateHasNamespace(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidatePodSpecSize(t *testing.T) {
	defaultPodSpec := &v1.PodSpec{
		Volumes: []v1.Volume{
			{
				Name: "foo",
			},
		},
	}

	defaultPodSpecSize := uint(len(protoutil.MustMarshall(defaultPodSpec)))

	tests := map[string]struct {
		req            *api.JobSubmitRequestItem
		expectSuccess  bool
		maxPodSpecSize uint
	}{
		"valid podspec in podspec": {
			req:            &api.JobSubmitRequestItem{PodSpec: defaultPodSpec},
			maxPodSpecSize: defaultPodSpecSize,
			expectSuccess:  true,
		},
		"valid podspec in podspecs": {
			req:            &api.JobSubmitRequestItem{PodSpecs: []*v1.PodSpec{defaultPodSpec}},
			maxPodSpecSize: defaultPodSpecSize,
			expectSuccess:  true,
		},
		"invalid podspec in podspec": {
			req:            &api.JobSubmitRequestItem{PodSpec: defaultPodSpec},
			maxPodSpecSize: defaultPodSpecSize - 1,
			expectSuccess:  false,
		},
		"invalid podspec in podspecs": {
			req:            &api.JobSubmitRequestItem{PodSpecs: []*v1.PodSpec{defaultPodSpec}},
			maxPodSpecSize: defaultPodSpecSize - 1,
			expectSuccess:  false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validatePodSpecSize(tc.req, configuration.SubmissionConfig{MaxPodSpecSizeBytes: tc.maxPodSpecSize})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidatePorts(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no ports": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{},
				},
			}},
			expectSuccess: true,
		},
		"single port": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
				},
			}},
			expectSuccess: true,
		},
		"multiple ports": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
							{ContainerPort: 8080},
						},
					},
				},
			}},
			expectSuccess: true,
		},
		"multiple containers": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 8080},
						},
					},
				},
			}},
			expectSuccess: true,
		},
		"duplicate port": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
							{ContainerPort: 80},
						},
					},
				},
			}},
			expectSuccess: false,
		},
		"duplicate port over multiple containers": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
				},
			}},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validatePorts(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidatePriorityClasses(t *testing.T) {
	defaultAllowedPriorityClasses := map[string]bool{
		"pc1": true,
	}

	tests := map[string]struct {
		req             *api.JobSubmitRequestItem
		priorityClasses map[string]bool
		expectSuccess   bool
	}{
		"empty priority class": {
			req:             &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{}},
			priorityClasses: defaultAllowedPriorityClasses,
			expectSuccess:   true,
		},
		"valid priority class": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				PriorityClassName: "pc1",
			}},
			priorityClasses: defaultAllowedPriorityClasses,
			expectSuccess:   true,
		},
		"invalid priority class": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				PriorityClassName: "notValid",
			}},
			priorityClasses: defaultAllowedPriorityClasses,
			expectSuccess:   false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validatePriorityClasses(tc.req, configuration.SubmissionConfig{
				AllowedPriorityClassNames: tc.priorityClasses,
			})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateClientId(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no client id": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: true,
		},
		"client id of  100 chars is fine": {
			req: &api.JobSubmitRequestItem{
				ClientId: strings.Repeat("a", 100),
			},
			expectSuccess: true,
		},
		"client id over 100 chars is forbidden": {
			req: &api.JobSubmitRequestItem{
				ClientId: strings.Repeat("a", 101),
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateClientId(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateQueue(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequest
		expectSuccess bool
	}{
		"good queue": {
			req: &api.JobSubmitRequest{
				Queue: "my-queue",
			},
			expectSuccess: true,
		},
		"missing queue": {
			req:           &api.JobSubmitRequest{},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateHasQueue(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateResources(t *testing.T) {
	oneCpu := v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("1"),
	}

	twoCpu := v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"),
	}

	tests := map[string]struct {
		req             *api.JobSubmitRequestItem
		minJobResources v1.ResourceList
		expectSuccess   bool
	}{
		"Requests Missing": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Limits: oneCpu,
				},
			}),
			expectSuccess: false,
		},
		"Limits Missing": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Requests: oneCpu,
				},
			}),
			expectSuccess: false,
		},
		"Requests and limits different": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Requests: oneCpu,
					Limits:   twoCpu,
				},
			}),
			expectSuccess: false,
		},
		"Request and limits the same": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Requests: oneCpu,
					Limits:   oneCpu,
				},
			}),
			expectSuccess: true,
		},
		"Request and limits the same with two containers": {
			req: reqFromContainers([]v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: oneCpu,
						Limits:   oneCpu,
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: twoCpu,
						Limits:   twoCpu,
					},
				},
			}),
			expectSuccess: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateResources(tc.req, configuration.SubmissionConfig{})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateTerminationGracePeriod(t *testing.T) {
	defaultMinPeriod := 30 * time.Second
	defaultMaxPeriod := 300 * time.Second

	tests := map[string]struct {
		req            *api.JobSubmitRequestItem
		minGracePeriod time.Duration
		maxGracePeriod time.Duration
		expectSuccess  bool
	}{
		"no period specified": {
			req:            &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  true,
		},
		"zero period specified": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(0),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  true,
		},
		"valid TerminationGracePeriod": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(60),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  true,
		},
		"TerminationGracePeriod too low": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(10),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  false,
		},
		"TerminationGracePeriod too high": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(700),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateTerminationGracePeriod(tc.req, configuration.SubmissionConfig{
				MinTerminationGracePeriod: tc.minGracePeriod,
				MaxTerminationGracePeriod: tc.maxGracePeriod,
			})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func reqFromContainer(container v1.Container) *api.JobSubmitRequestItem {
	return reqFromContainers([]v1.Container{container})
}

func reqFromContainers(containers []v1.Container) *api.JobSubmitRequestItem {
	return &api.JobSubmitRequestItem{
		PodSpec: &v1.PodSpec{Containers: containers},
	}
}

func podSpecFromNodeSelector(requirement v1.NodeSelectorRequirement) *v1.PodSpec {
	return &v1.PodSpec{
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{requirement},
						},
					},
				},
				PreferredDuringSchedulingIgnoredDuringExecution: nil,
			},
		},
	}
}

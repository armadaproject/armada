package util

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/domain"
)

func TestBindJsonOrYaml_Yaml(t *testing.T) {
	submitFile := &domain.JobSubmitFile{}
	err := BindJsonOrYaml(filepath.Join("testdata", "jobs.yaml"), submitFile)
	assert.NoError(t, err)
	assert.Equal(t, getExpectedJobSubmitFile(t), submitFile)
}

func TestBindJsonOrYaml_Json(t *testing.T) {
	submitFile := &domain.JobSubmitFile{}
	err := BindJsonOrYaml(filepath.Join("testdata", "jobs.json"), submitFile)
	assert.NoError(t, err)
	assert.Equal(t, getExpectedJobSubmitFile(t), submitFile)
}

func TestBindJsonOrYaml_IngressType(t *testing.T) {
	submitFile := &domain.JobSubmitFile{}
	err := BindJsonOrYaml(filepath.Join("testdata", "jobs-ingress.yaml"), submitFile)
	assert.NoError(t, err)
	assert.Equal(t, submitFile.Jobs[0].Services[0].Type, api.ServiceType_NodePort)
	assert.Equal(t, submitFile.Jobs[0].Services[1].Type, api.ServiceType_NodePort)
}

func getExpectedJobSubmitFile(t *testing.T) *domain.JobSubmitFile {
	return &domain.JobSubmitFile{
		Queue:    "test",
		JobSetId: "job-set-1",
		Jobs: []*api.JobSubmitRequestItem{
			{
				Priority: 0,
				PodSpec: &v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:            "sleep",
							ImagePullPolicy: v1.PullIfNotPresent,
							Image:           "alpine:latest",
							Command:         []string{"sh", "-c"},
							Args:            []string{"sleep 60"},
							Resources: v1.ResourceRequirements{
								Limits:   makeResourceList(t, "1", "1Gi"),
								Requests: makeResourceList(t, "1", "1Gi"),
							},
						},
					},
				},
			},
		},
	}
}

func makeResourceList(t *testing.T, cpu string, memory string) v1.ResourceList {
	cpuResource, err := resource.ParseQuantity(cpu)
	assert.NoError(t, err)
	memoryResource, err := resource.ParseQuantity(memory)
	assert.NoError(t, err)
	return v1.ResourceList{v1.ResourceCPU: cpuResource, v1.ResourceMemory: memoryResource}
}

package service

import (
	"testing"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/executor/domain"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreateLabels_CreatesExpectedLabels(t *testing.T) {
	job := api.Job{
		Id:       "Id",
		JobSetId: "JobSetId",
		Queue:    "Queue1",
		PodSpec:  makePodSpec(),
	}

	expectedLabels := map[string]string{
		domain.Queue: job.Queue,
	}

	expectedAnotations := map[string]string{
		domain.JobId:    job.Id,
		domain.JobSetId: job.JobSetId,
	}

	result := createPod(&job)

	assert.Equal(t, result.Labels, expectedLabels)
	assert.Equal(t, result.Annotations, expectedAnotations)
}

func TestSetRestartPolicyNever_OverwritesExistingValue(t *testing.T) {
	podSpec := makePodSpec()

	podSpec.RestartPolicy = v1.RestartPolicyAlways
	assert.Equal(t, podSpec.RestartPolicy, v1.RestartPolicyAlways)

	setRestartPolicyNever(podSpec)
	assert.Equal(t, podSpec.RestartPolicy, v1.RestartPolicyNever)
}

func TestCreatePod_CreatesExpectedPod(t *testing.T) {
	podSpec := makePodSpec()
	job := api.Job{
		Id:          "Id",
		JobSetId:    "JobSetId",
		Queue:       "Queue1",
		PodSpec:     podSpec,
		Annotations: map[string]string{"annotation": "test"},
		Labels:      map[string]string{"label": "test"},
	}

	podSpec.RestartPolicy = v1.RestartPolicyNever

	expectedOutput := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: PodNamePrefix + job.Id,
			Labels: map[string]string{
				domain.Queue: job.Queue,
				"label":      "test",
			},
			Annotations: map[string]string{
				domain.JobId:    job.Id,
				domain.JobSetId: job.JobSetId,
				"annotation":    "test",
			},
		},
		Spec: *podSpec,
	}

	result := createPod(&job)
	assert.Equal(t, result, &expectedOutput)
}

func makePodSpec() *v1.PodSpec {
	containers := make([]v1.Container, 1)
	containers[0] = v1.Container{
		Name:  "Container1",
		Image: "index.docker.io/library/ubuntu:latest",
		Args:  []string{"sleep", "10s"},
	}
	spec := v1.PodSpec{
		NodeName:   "NodeName",
		Containers: containers,
	}

	return &spec
}

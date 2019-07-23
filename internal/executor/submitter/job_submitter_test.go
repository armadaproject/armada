package submitter

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCreateLabels_CreatesExpectedLabels(t *testing.T) {
	job := api.Job{
		Id:       "Id",
		JobSetId: "JobSetId",
		Queue:    "Queue1",
	}

	expectedOutput := map[string]string{
		domain.JobId:           job.Id,
		domain.JobSetId:        job.JobSetId,
		domain.Queue:           job.Queue,
		domain.ReadyForCleanup: "false",
	}

	result := createLabels(&job)

	assert.Equal(t, result, expectedOutput)
}

func TestCreatePod_CreatesExpectedPod(t *testing.T) {
	job := api.Job{
		Id:       "Id",
		JobSetId: "JobSetId",
		Queue:    "Queue1",
		PodSpec:  makePodSpec(),
	}

	labels := createLabels(&job)

	expectedOutput := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   PodNamePrefix + job.Id,
			Labels: labels,
		},
		Spec: *job.PodSpec,
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

package service

import (
	"testing"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/pkg/api"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreateLabels_CreatesExpectedLabels(t *testing.T) {
	job := api.Job{
		Id:       "Id",
		JobSetId: "JobSetId",
		Queue:    "Queue1",
		Owner:    "Owner",
		PodSpec:  makePodSpec(),
	}

	expectedLabels := map[string]string{
		domain.JobId:     job.Id,
		domain.Queue:     job.Queue,
		domain.PodNumber: "0",
		domain.PodCount:  "1",
	}

	expectedAnnotations := map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	}

	result := createPod(&job, &configuration.PodDefaults{}, 0)

	assert.Equal(t, result.Labels, expectedLabels)
	assert.Equal(t, result.Annotations, expectedAnnotations)
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
		Owner:       "User1",
		PodSpec:     podSpec,
		Annotations: map[string]string{"annotation": "test"},
		Labels:      map[string]string{"label": "test"},
	}

	podSpec.RestartPolicy = v1.RestartPolicyNever

	expectedOutput := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: common.PodNamePrefix + job.Id + "-0",
			Labels: map[string]string{
				domain.JobId:     job.Id,
				domain.Queue:     job.Queue,
				domain.PodNumber: "0",
				domain.PodCount:  "1",
				"label":          "test",
			},
			Annotations: map[string]string{
				domain.JobSetId: job.JobSetId,
				domain.Owner:    job.Owner,
				"annotation":    "test",
			},
		},
		Spec: *podSpec,
	}

	result := createPod(&job, &configuration.PodDefaults{}, 0)
	assert.Equal(t, result, &expectedOutput)
}

func TestApplyDefaults(t *testing.T) {
	schedulerName := "OtherScheduler"

	podSpec := makePodSpec()
	expected := podSpec.DeepCopy()
	expected.SchedulerName = schedulerName

	applyDefaults(podSpec, &configuration.PodDefaults{SchedulerName: schedulerName})
	assert.Equal(t, expected, podSpec)
}

func TestApplyDefaults_HandleEmptyDefaults(t *testing.T) {
	podSpecOriginal := makePodSpec()
	podSpec := podSpecOriginal.DeepCopy()

	applyDefaults(podSpec, nil)
	assert.Equal(t, podSpecOriginal, podSpec)

	applyDefaults(podSpec, &configuration.PodDefaults{})
	assert.Equal(t, podSpecOriginal, podSpec)
}

func TestApplyDefaults_DoesNotOverrideExistingValues(t *testing.T) {
	podSpecOriginal := makePodSpec()
	podSpecOriginal.SchedulerName = "Scheduler"

	podSpec := podSpecOriginal.DeepCopy()
	applyDefaults(podSpec, &configuration.PodDefaults{SchedulerName: "OtherScheduler"})
	assert.Equal(t, podSpecOriginal, podSpec)
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

package util

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/armadaproject/armada/internal/common/util"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
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

	result := CreatePod(&job, &configuration.PodDefaults{})

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

	result := CreatePod(&job, &configuration.PodDefaults{})
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

func TestCreatePodFromExecutorApiJob(t *testing.T) {
	runId := uuid.NewString()
	jobId := util.NewULID()
	runIndex := 0

	validJobLease := &executorapi.JobRunLease{
		JobRunId:    runId,
		JobRunIndex: uint32(runIndex),
		Queue:       "queue",
		Jobset:      "job-set",
		User:        "user",
		Job: &armadaevents.SubmitJob{
			ObjectMeta: &armadaevents.ObjectMeta{
				Labels:      map[string]string{},
				Annotations: map[string]string{"runtime_gang_cardinality": "3"},
				Namespace:   "test-namespace",
			},
			JobId: jobId,
			MainObject: &armadaevents.KubernetesMainObject{
				Object: &armadaevents.KubernetesMainObject_PodSpec{
					PodSpec: &armadaevents.PodSpecWithAvoidList{
						PodSpec: &v1.PodSpec{},
					},
				},
			},
		},
	}

	expectedPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("armada-%s-0-%d", jobId, runIndex),
			Namespace: "test-namespace",
			Labels: map[string]string{
				domain.JobId:       jobId,
				domain.JobRunId:    runId,
				domain.JobRunIndex: strconv.Itoa(runIndex),
				domain.Queue:       "queue",
				domain.PodNumber:   "0",
				domain.PodCount:    "1",
			},
			Annotations: map[string]string{
				domain.JobSetId:            "job-set",
				domain.Owner:               "user",
				"runtime_gang_cardinality": "3",
			},
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			SchedulerName: "scheduler-name",
		},
	}

	result, err := CreatePodFromExecutorApiJob(validJobLease, &configuration.PodDefaults{SchedulerName: "scheduler-name"})
	assert.NoError(t, err)
	assert.Equal(t, expectedPod, result)
}

func TestCreatePodFromExecutorApiJob_Invalid(t *testing.T) {
	lease := createBasicJobRunLease()
	_, err := CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{})
	assert.NoError(t, err)

	// Invalid run id
	lease = createBasicJobRunLease()
	lease.JobRunId = ""
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{})
	assert.Error(t, err)

	// Invalid job id
	lease = createBasicJobRunLease()
	lease.Job.JobId = ""
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{})
	assert.Error(t, err)

	// no pod spec
	lease = createBasicJobRunLease()
	lease.Job.MainObject = &armadaevents.KubernetesMainObject{}
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{})
	assert.Error(t, err)
}

func createBasicJobRunLease() *executorapi.JobRunLease {
	return &executorapi.JobRunLease{
		JobRunId: uuid.NewString(),
		Queue:    "queue",
		Jobset:   "job-set",
		User:     "user",
		Job: &armadaevents.SubmitJob{
			ObjectMeta: &armadaevents.ObjectMeta{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
				Namespace:   "test-namespace",
			},
			JobId: util.NewULID(),
			MainObject: &armadaevents.KubernetesMainObject{
				Object: &armadaevents.KubernetesMainObject_PodSpec{
					PodSpec: &armadaevents.PodSpecWithAvoidList{
						PodSpec: &v1.PodSpec{},
					},
				},
			},
		},
	}
}

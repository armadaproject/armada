package util

import (
	"fmt"
	"testing"

	"github.com/armadaproject/armada/internal/common/util"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	serverconfiguration "github.com/armadaproject/armada/internal/server/configuration"
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

	validJobLease := &executorapi.JobRunLease{
		JobRunId: runId,
		Queue:    "queue",
		Jobset:   "job-set",
		User:     "user",
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
						PodSpec: &v1.PodSpec{
							Containers: []v1.Container{{Name: "test", Image: "test"}},
						},
					},
				},
			},
		},
	}

	expectedEnvVars := []v1.EnvVar{
		{Name: serverconfiguration.JobIdEnvVar, Value: jobId},
		{Name: serverconfiguration.QueueEnvVar, Value: "queue"},
		{Name: serverconfiguration.JobSetIdEnvVar, Value: "job-set"},
	}
	expectedPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("armada-%s-0", jobId),
			Namespace: "test-namespace",
			Labels: map[string]string{
				domain.JobId:     jobId,
				domain.JobRunId:  runId,
				domain.Queue:     "queue",
				domain.PodNumber: "0",
				domain.PodCount:  "1",
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
			Containers:    []v1.Container{{Name: "test", Image: "test", Env: expectedEnvVars}},
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

func TestInjectArmadaEnvVars(t *testing.T) {
	tests := []struct {
		name                      string
		jobId                     string
		queue                     string
		jobsetId                  string
		annotations               map[string]string
		existingEnvs              []v1.EnvVar
		wantEnvs                  map[string]string
		dontWantEnvs              []string
		wantInitContainerEnvs     map[string]string
		dontWantInitContainerEnvs []string
	}{
		{
			name:     "injects base env vars for non-gang job",
			jobId:    "job-123",
			queue:    "test-queue",
			jobsetId: "jobset-456",
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.QueueEnvVar:    "test-queue",
				serverconfiguration.JobSetIdEnvVar: "jobset-456",
			},
			dontWantEnvs: []string{
				serverconfiguration.GangIdEnvVar,
				serverconfiguration.GangCardinalityEnvVar,
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.QueueEnvVar:    "test-queue",
				serverconfiguration.JobSetIdEnvVar: "jobset-456",
			},
			dontWantInitContainerEnvs: []string{
				serverconfiguration.GangIdEnvVar,
				serverconfiguration.GangCardinalityEnvVar,
			},
		},
		{
			name:     "preserves user-defined env vars",
			jobId:    "new-job",
			queue:    "new-queue",
			jobsetId: "new-jobset",
			existingEnvs: []v1.EnvVar{
				{Name: serverconfiguration.JobIdEnvVar, Value: "existing-job"},
			},
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "existing-job", // preserved in main container
				serverconfiguration.QueueEnvVar:    "new-queue",
				serverconfiguration.JobSetIdEnvVar: "new-jobset",
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "new-job", // init container gets new value
				serverconfiguration.QueueEnvVar:    "new-queue",
				serverconfiguration.JobSetIdEnvVar: "new-jobset",
			},
		},
		{
			name:     "injects all gang-related env vars for fully configured gang job",
			jobId:    "job-123",
			queue:    "queue",
			jobsetId: "jobset",
			annotations: map[string]string{
				serverconfiguration.GangIdAnnotation:                   "gang-789",
				serverconfiguration.GangCardinalityAnnotation:          "3",
				serverconfiguration.GangNodeUniformityLabelNameEnvVar:  "rack",
				serverconfiguration.GangNodeUniformityLabelValueEnvVar: "rack-1",
			},
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:                        "job-123",
				serverconfiguration.QueueEnvVar:                        "queue",
				serverconfiguration.JobSetIdEnvVar:                     "jobset",
				serverconfiguration.GangIdEnvVar:                       "gang-789",
				serverconfiguration.GangCardinalityEnvVar:              "3",
				serverconfiguration.GangNodeUniformityLabelNameEnvVar:  "rack",
				serverconfiguration.GangNodeUniformityLabelValueEnvVar: "rack-1",
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:                        "job-123",
				serverconfiguration.QueueEnvVar:                        "queue",
				serverconfiguration.JobSetIdEnvVar:                     "jobset",
				serverconfiguration.GangIdEnvVar:                       "gang-789",
				serverconfiguration.GangCardinalityEnvVar:              "3",
				serverconfiguration.GangNodeUniformityLabelNameEnvVar:  "rack",
				serverconfiguration.GangNodeUniformityLabelValueEnvVar: "rack-1",
			},
		},
		{
			name:     "skips node uniformity env vars when only label name annotation exists",
			jobId:    "job-123",
			queue:    "queue",
			jobsetId: "jobset",
			annotations: map[string]string{
				serverconfiguration.GangNodeUniformityLabelNameEnvVar: "rack",
			},
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.QueueEnvVar:    "queue",
				serverconfiguration.JobSetIdEnvVar: "jobset",
			},
			dontWantEnvs: []string{
				serverconfiguration.GangNodeUniformityLabelNameEnvVar,
				serverconfiguration.GangNodeUniformityLabelValueEnvVar,
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.QueueEnvVar:    "queue",
				serverconfiguration.JobSetIdEnvVar: "jobset",
			},
			dontWantInitContainerEnvs: []string{
				serverconfiguration.GangNodeUniformityLabelNameEnvVar,
				serverconfiguration.GangNodeUniformityLabelValueEnvVar,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			podSpec := &v1.PodSpec{
				Containers:     []v1.Container{{Name: "main", Env: tc.existingEnvs}},
				InitContainers: []v1.Container{{Name: "init"}},
			}

			injectArmadaEnvVars(podSpec, tc.jobId, tc.queue, tc.jobsetId, tc.annotations)

			for _, container := range podSpec.InitContainers {
				envMap := make(map[string]string, len(container.Env))
				for _, env := range container.Env {
					envMap[env.Name] = env.Value
				}

				for name, value := range tc.wantInitContainerEnvs {
					assert.Equal(t, value, envMap[name])
				}
				for _, name := range tc.dontWantInitContainerEnvs {
					assert.NotContains(t, envMap, name)
				}
			}
			for _, container := range podSpec.Containers {
				envMap := make(map[string]string, len(container.Env))
				for _, env := range container.Env {
					envMap[env.Name] = env.Value
				}
				for name, value := range tc.wantEnvs {
					assert.Equal(t, value, envMap[name])
				}
				for _, name := range tc.dontWantEnvs {
					assert.NotContains(t, envMap, name)
				}
			}
		})
	}
}

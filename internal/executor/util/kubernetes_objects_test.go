package util

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	serverconfiguration "github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestSetRestartPolicyNever_OverwritesExistingValue(t *testing.T) {
	podSpec := makePodSpec()

	podSpec.RestartPolicy = v1.RestartPolicyAlways
	assert.Equal(t, podSpec.RestartPolicy, v1.RestartPolicyAlways)

	setRestartPolicyNever(podSpec)
	assert.Equal(t, podSpec.RestartPolicy, v1.RestartPolicyNever)
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
				Annotations: map[string]string{"runtime_gang_cardinality": "3", serverconfiguration.ObjectNamePrefixAnnotation: "team"},
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
		{Name: serverconfiguration.JobRunIdEnvVar, Value: runId},
		{Name: serverconfiguration.QueueEnvVar, Value: "queue"},
		{Name: serverconfiguration.JobSetIdEnvVar, Value: "job-set"},
	}
	expectedPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("team-%s-0", jobId),
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
				serverconfiguration.ObjectNamePrefixAnnotation: "team",
			},
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			SchedulerName: "scheduler-name",
			Containers:    []v1.Container{{Name: "test", Image: "test", Env: expectedEnvVars}},
		},
	}

	result, err := CreatePodFromExecutorApiJob(validJobLease, &configuration.PodDefaults{SchedulerName: "scheduler-name"}, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedPod, result)
}

func TestCreatePodFromExecutorApiJob_Invalid(t *testing.T) {
	lease := createBasicJobRunLease()
	_, err := CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{}, false)
	assert.NoError(t, err)

	// Invalid run id
	lease = createBasicJobRunLease()
	lease.JobRunId = ""
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{}, false)
	assert.Error(t, err)

	// Invalid job id
	lease = createBasicJobRunLease()
	lease.Job.JobId = ""
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{}, false)
	assert.Error(t, err)

	// no pod spec
	lease = createBasicJobRunLease()
	lease.Job.MainObject = &armadaevents.KubernetesMainObject{}
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{}, false)
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
		runId                     string
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
			runId:    "run-100",
			queue:    "test-queue",
			jobsetId: "jobset-456",
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.JobRunIdEnvVar: "run-100",
				serverconfiguration.QueueEnvVar:    "test-queue",
				serverconfiguration.JobSetIdEnvVar: "jobset-456",
			},
			dontWantEnvs: []string{
				serverconfiguration.GangIdEnvVar,
				serverconfiguration.GangCardinalityEnvVar,
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.JobRunIdEnvVar: "run-100",
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
			runId:    "run-200",
			queue:    "new-queue",
			jobsetId: "new-jobset",
			existingEnvs: []v1.EnvVar{
				{Name: serverconfiguration.JobIdEnvVar, Value: "existing-job"},
				{Name: serverconfiguration.JobRunIdEnvVar, Value: "existing-run"},
			},
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "existing-job", // preserved in main container
				serverconfiguration.JobRunIdEnvVar: "existing-run",
				serverconfiguration.QueueEnvVar:    "new-queue",
				serverconfiguration.JobSetIdEnvVar: "new-jobset",
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "new-job", // init container gets new value
				serverconfiguration.JobRunIdEnvVar: "run-200",
				serverconfiguration.QueueEnvVar:    "new-queue",
				serverconfiguration.JobSetIdEnvVar: "new-jobset",
			},
		},
		{
			name:     "injects all gang-related env vars for fully configured gang job",
			jobId:    "job-123",
			runId:    "run-300",
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
				serverconfiguration.JobRunIdEnvVar:                     "run-300",
				serverconfiguration.QueueEnvVar:                        "queue",
				serverconfiguration.JobSetIdEnvVar:                     "jobset",
				serverconfiguration.GangIdEnvVar:                       "gang-789",
				serverconfiguration.GangCardinalityEnvVar:              "3",
				serverconfiguration.GangNodeUniformityLabelNameEnvVar:  "rack",
				serverconfiguration.GangNodeUniformityLabelValueEnvVar: "rack-1",
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:                        "job-123",
				serverconfiguration.JobRunIdEnvVar:                     "run-300",
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
			runId:    "run-400",
			queue:    "queue",
			jobsetId: "jobset",
			annotations: map[string]string{
				serverconfiguration.GangNodeUniformityLabelNameEnvVar: "rack",
			},
			wantEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.JobRunIdEnvVar: "run-400",
				serverconfiguration.QueueEnvVar:    "queue",
				serverconfiguration.JobSetIdEnvVar: "jobset",
			},
			dontWantEnvs: []string{
				serverconfiguration.GangNodeUniformityLabelNameEnvVar,
				serverconfiguration.GangNodeUniformityLabelValueEnvVar,
			},
			wantInitContainerEnvs: map[string]string{
				serverconfiguration.JobIdEnvVar:    "job-123",
				serverconfiguration.JobRunIdEnvVar: "run-400",
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

			lease := &executorapi.JobRunLease{
				JobRunId: tc.runId,
				Queue:    tc.queue,
				Jobset:   tc.jobsetId,
				Job:      &armadaevents.SubmitJob{JobId: tc.jobId},
			}
			injectArmadaEnvVars(podSpec, lease, tc.annotations)

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

func TestPodName(t *testing.T) {
	tests := []struct {
		name              string
		prefix            string
		runScopedPodNames bool
		want              string
	}{
		{name: "the flag off gives <prefix>-<jobId>-0", prefix: "team", want: "team-job-1-0"},
		{name: "the flag on gives <prefix>-<runId>", prefix: "team", runScopedPodNames: true, want: "team-run-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, PodName(tc.prefix, "job-1", "run-1", tc.runScopedPodNames))
		})
	}
}

func TestObjectNamePrefix(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{name: "the annotation gives the prefix", annotations: map[string]string{serverconfiguration.ObjectNamePrefixAnnotation: "team"}, want: "team"},
		{name: "a job without the annotation gets the default prefix", annotations: map[string]string{"other": "value"}, want: "armada"},
		{name: "a job without annotations gets the default prefix", want: "armada"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ObjectNamePrefix(tc.annotations))
		})
	}
}

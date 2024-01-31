package util

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
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

func makeTestJob() *api.Job {
	return &api.Job{
		Id:        "Id",
		JobSetId:  "JobSetId",
		Queue:     "QueueTest",
		Owner:     "UserTest",
		Namespace: "testNamespace",
		PodSpecs:  []*v1.PodSpec{makePodSpec()},
	}
}

func makeTestService() *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "testService"},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name: "testPort",
					Port: 8080,
				},
			},
		},
	}
}

func TestCreateIngress_Basic(t *testing.T) {
	// Boilerplate, should be the same in TlsEnabled
	job := makeTestJob()
	service := makeTestService()
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "testPod", Namespace: "testNamespace"}}
	ingressConfig := &configuration.IngressConfiguration{
		HostnameSuffix: "testSuffix",
	}

	// TLS disabled jobconfig
	jobConfig := &IngressServiceConfig{
		Ports: []uint32{8080},
	}

	result := CreateIngress("testIngress", job, pod, service, ingressConfig, jobConfig)

	pathType := networking.PathTypePrefix
	expectedIngressSpec := networking.IngressSpec{
		TLS: []networking.IngressTLS{},
		Rules: []networking.IngressRule{
			{
				Host: "testPort-testPod.testNamespace.testSuffix",
				IngressRuleValue: networking.IngressRuleValue{
					HTTP: &networking.HTTPIngressRuleValue{
						Paths: []networking.HTTPIngressPath{
							{
								Path:     "/",
								PathType: &pathType,
								Backend: networking.IngressBackend{
									Service: &networking.IngressServiceBackend{
										Name: "testService",
										Port: networking.ServiceBackendPort{
											Number: 8080,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, result.Spec, expectedIngressSpec)
}

func TestCreateIngress_TLS(t *testing.T) {
	// Boilerplate setup
	job := makeTestJob()
	service := makeTestService()
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "testPod", Namespace: "testNamespace"}}
	ingressConfig := &configuration.IngressConfiguration{
		HostnameSuffix: "testSuffix",
		CertNameSuffix: "ingress-tls-certificate",
	}

	// TLS enabled in this test
	jobConfig := &IngressServiceConfig{
		TlsEnabled: true,
		Ports:      []uint32{8080},
	}

	result := CreateIngress("testIngress", job, pod, service, ingressConfig, jobConfig)

	pathType := networking.PathTypePrefix
	expectedIngressSpec := networking.IngressSpec{
		TLS: []networking.IngressTLS{
			{
				Hosts: []string{
					"testPort-testPod.testNamespace.testSuffix",
				},
				SecretName: "testNamespace-ingress-tls-certificate",
			},
		},
		Rules: []networking.IngressRule{
			{
				Host: "testPort-testPod.testNamespace.testSuffix",
				IngressRuleValue: networking.IngressRuleValue{
					HTTP: &networking.HTTPIngressRuleValue{
						Paths: []networking.HTTPIngressPath{
							{
								Path:     "/",
								PathType: &pathType,
								Backend: networking.IngressBackend{
									Service: &networking.IngressServiceBackend{
										Name: "testService",
										Port: networking.ServiceBackendPort{
											Number: 8080,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, result.Spec, expectedIngressSpec)
}

func TestCreateService_Ingress_Headless(t *testing.T) {
	job := makeTestJob()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
		},
	}
	ports := []v1.ServicePort{
		{
			Port: 123,
		},
	}
	ingressType := Ingress
	createdService := CreateService(job, pod, ports, ingressType, false)

	expected := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod-ingress",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Annotations: map[string]string{
				"armada_jobset_id": "JobSetId",
				"armada_owner":     "UserTest",
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Port: 123,
				},
			},
			Selector: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Type:      "ClusterIP",
			ClusterIP: "None",
		},
	}
	assert.Equal(t, createdService, expected)
}

func TestCreateService_Ingress_ClusterIP(t *testing.T) {
	job := makeTestJob()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
		},
	}
	ports := []v1.ServicePort{
		{
			Port: 123,
		},
	}
	ingressType := Ingress
	createdService := CreateService(job, pod, ports, ingressType, true)

	expected := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod-ingress",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Annotations: map[string]string{
				"armada_jobset_id": "JobSetId",
				"armada_owner":     "UserTest",
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Port: 123,
				},
			},
			Selector: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Type: "ClusterIP",
		},
	}
	assert.Equal(t, createdService, expected)
}

func TestCreateService_NodePort(t *testing.T) {
	job := makeTestJob()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
		},
	}
	ports := []v1.ServicePort{
		{
			Port:     123,
			NodePort: 456,
		},
	}
	ingressType := NodePort
	createdService := CreateService(job, pod, ports, ingressType, true)

	expected := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod-nodeport",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Annotations: map[string]string{
				"armada_jobset_id": "JobSetId",
				"armada_owner":     "UserTest",
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Port:     123,
					NodePort: 456,
				},
			},
			Selector: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Type: "NodePort",
		},
	}
	assert.Equal(t, createdService, expected)
}

func TestCreateService_Headless(t *testing.T) {
	job := makeTestJob()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
		},
	}
	ports := []v1.ServicePort{
		{
			Port: 123,
		},
	}
	ingressType := Headless
	createdService := CreateService(job, pod, ports, ingressType, false)

	expected := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod-headless",
			Namespace: "testNamespace",
			Labels: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Annotations: map[string]string{
				"armada_jobset_id": "JobSetId",
				"armada_owner":     "UserTest",
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Port: 123,
				},
			},
			Selector: map[string]string{
				"armada_job_id":     "test_id",
				"armada_pod_number": "0",
				"armada_queue_id":   "test_queue_id",
			},
			Type:      "ClusterIP",
			ClusterIP: "None",
		},
	}
	assert.Equal(t, createdService, expected)
}

func TestCreatePodFromExecutorApiJob(t *testing.T) {
	runId := armadaevents.ProtoUuidFromUuid(uuid.New())
	runIdStr, err := armadaevents.UuidStringFromProtoUuid(runId)
	assert.NoError(t, err)
	jobId := armadaevents.ProtoUuidFromUuid(uuid.New())
	jobIdStr, err := armadaevents.UlidStringFromProtoUuid(jobId)
	assert.NoError(t, err)

	validJobLease := &executorapi.JobRunLease{
		JobRunId: runId,
		Queue:    "queue",
		Jobset:   "job-set",
		User:     "user",
		Job: &armadaevents.SubmitJob{
			ObjectMeta: &armadaevents.ObjectMeta{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
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
			Name:      fmt.Sprintf("armada-%s-0", jobIdStr),
			Namespace: "test-namespace",
			Labels: map[string]string{
				domain.JobId:     jobIdStr,
				domain.JobRunId:  runIdStr,
				domain.Queue:     "queue",
				domain.PodNumber: "0",
				domain.PodCount:  "1",
			},
			Annotations: map[string]string{
				domain.JobSetId: "job-set",
				domain.Owner:    "user",
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
	lease.JobRunId = nil
	_, err = CreatePodFromExecutorApiJob(lease, &configuration.PodDefaults{})
	assert.Error(t, err)

	// Invalid job id
	lease = createBasicJobRunLease()
	lease.Job.JobId = nil
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
		JobRunId: armadaevents.ProtoUuidFromUuid(uuid.New()),
		Queue:    "queue",
		Jobset:   "job-set",
		User:     "user",
		Job: &armadaevents.SubmitJob{
			ObjectMeta: &armadaevents.ObjectMeta{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
				Namespace:   "test-namespace",
			},
			JobId: armadaevents.ProtoUuidFromUuid(uuid.New()),
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

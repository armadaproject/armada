package eventutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestConvertLogObjectMeta(t *testing.T) {
	expected := &armadaevents.ObjectMeta{
		ExecutorId:   "", // Can't be part of the test since the k8s ObjectMeta doesn't include it.
		Namespace:    "namespace",
		Name:         "name",
		KubernetesId: "id",
		Annotations:  map[string]string{"annotation_1": "annotation_1", "annotation_2": "annotation_2"},
		Labels:       map[string]string{"label_1": "label_1", "label_2": "label_2"},
	}
	k8sObjectMeta := K8sObjectMetaFromLogObjectMeta(expected)
	actual := LogObjectMetaFromK8sObjectMeta(k8sObjectMeta)
	assert.Equal(t, expected, actual)
}

func TestConvertK8sObjectMeta(t *testing.T) {
	expected := &metav1.ObjectMeta{
		Namespace:   "namespace",
		Name:        "name",
		UID:         "id",
		Annotations: map[string]string{"annotation_1": "annotation_1", "annotation_2": "annotation_2"},
		Labels:      map[string]string{"label_1": "label_1", "label_2": "label_2"},
	}
	logObjectMeta := LogObjectMetaFromK8sObjectMeta(expected)
	actual := K8sObjectMetaFromLogObjectMeta(logObjectMeta)
	assert.Equal(t, expected, actual)
}

func TestConvertJobErrors(t *testing.T) {
	apiJob := testJob(false)
	apiJob.PodSpec = nil
	apiJob.PodSpecs = nil
	_, err := LogSubmitJobFromApiJob(apiJob)
	assert.Error(t, err)
}

func TestK8sServicesIngressesFromApiJob(t *testing.T) {
	apiJob := testJob(false)
	ingressConfig := &configuration.IngressConfiguration{
		HostnameSuffix: "HostnameSuffix",
		CertNameSuffix: "CertNameSuffix",
		Annotations:    map[string]string{"ingress_annotation_1": "ingress_annotation_1", "ingress_annotation_2": "ingress_annotation_2"},
	}

	services, ingresses, err := K8sServicesIngressesFromApiJob(apiJob, ingressConfig)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 3, len(services)) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, len(ingresses)) {
		t.FailNow()
	}

	expectedLabels := util.MergeMaps(
		apiJob.Labels,
		map[string]string{"armada_job_id": apiJob.GetId(), "armada_pod_number": "0", "armada_queue_id": apiJob.GetQueue()},
	)
	expectedServiceAnnotations := util.MergeMaps(
		apiJob.Annotations,
		map[string]string{"armada_jobset_id": apiJob.GetJobSetId(), "armada_owner": apiJob.GetOwner()},
	)

	expectedServices := make(map[string]*v1.Service)
	for _, suffix := range []string{"ingress", "nodeport", "headless"} {
		name := fmt.Sprintf("armada-%s-0-%s", apiJob.GetId(), suffix)
		port := int32(5000)
		clusterIP := v1.ClusterIPNone
		serviceType := v1.ServiceTypeClusterIP
		if suffix == "nodeport" {
			port = 6000
			clusterIP = ""
			serviceType = v1.ServiceTypeNodePort
		} else if suffix == "headless" {
			port = 7000
		}
		expectedServices[name] = &v1.Service{
			TypeMeta: metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("armada-%s-0-%s", apiJob.GetId(), suffix),
				Namespace:       apiJob.Namespace,
				Labels:          expectedLabels,
				Annotations:     expectedServiceAnnotations,
				OwnerReferences: nil,
				Finalizers:      nil,
				ManagedFields:   nil,
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{
					{
						Name:     fmt.Sprintf("%s_%s-%d", "podSpec", "container1", port),
						Protocol: v1.ProtocolTCP,
						Port:     port,
					},
					{
						Name:     fmt.Sprintf("%s_%s-%d", "podSpec", "container2", port),
						Protocol: v1.ProtocolTCP,
						Port:     port,
					},
				},
				Selector: map[string]string{
					"armada_job_id": apiJob.GetId(), "armada_pod_number": "0", "armada_queue_id": "queue",
				},
				ClusterIP: clusterIP,
				Type:      serviceType,
			},
		}
	}

	expectedIngressAnnotations := util.MergeMaps(
		expectedServiceAnnotations,
		ingressConfig.Annotations,
	)

	expectedIngressRules := make([]networking.IngressRule, 2)
	pathType := networking.PathTypeImplementationSpecific
	for i, container := range apiJob.PodSpec.Containers {
		expectedIngressRules[i] = networking.IngressRule{
			Host: fmt.Sprintf("%s-%d-armada-%s-0.%s.%s",
				container.Name, 5000, apiJob.GetId(), apiJob.GetNamespace(), ingressConfig.HostnameSuffix),
			IngressRuleValue: networking.IngressRuleValue{
				HTTP: &networking.HTTPIngressRuleValue{
					Paths: []networking.HTTPIngressPath{
						{
							Path:     "/",
							PathType: &pathType,
							Backend: networking.IngressBackend{
								Service: &networking.IngressServiceBackend{
									Name: fmt.Sprintf("armada-%s-0-ingress", apiJob.GetId()),
									Port: networking.ServiceBackendPort{
										Number: 5000,
									},
								},
							},
						},
					},
				},
			},
		}
	}

	expectedIngress := &networking.Ingress{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("armada-%s-%d-ingress-%d", apiJob.GetId(), 0, 0),
			Namespace:   apiJob.GetNamespace(),
			Labels:      expectedLabels,
			Annotations: expectedIngressAnnotations,
		},
		Spec: networking.IngressSpec{
			IngressClassName: nil,
			DefaultBackend:   nil,
			TLS: []networking.IngressTLS{
				{
					Hosts: []string{
						fmt.Sprintf("podSpec_container1-5000-armada-%s-0.%s.%s", apiJob.GetId(), apiJob.GetNamespace(), ingressConfig.HostnameSuffix),
						fmt.Sprintf("podSpec_container2-5000-armada-%s-0.%s.%s", apiJob.GetId(), apiJob.GetNamespace(), ingressConfig.HostnameSuffix),
					},
					SecretName: fmt.Sprintf("%s-%s", apiJob.Namespace, ingressConfig.CertNameSuffix),
				},
			},
			Rules: expectedIngressRules,
		},
		Status: networking.IngressStatus{},
	}

	for _, service := range services {
		expected, ok := expectedServices[service.Name]
		if !assert.Truef(t, ok, "got unexpected service name %s", service.Name) {
			t.FailNow()
		}
		assert.Equal(t, expected, service)
	}

	if !assert.Equal(t, expectedIngress, ingresses[0]) {
		t.FailNow()
	}
}

func TestEventSequenceFromApiEvent_Preempted(t *testing.T) {
	testEvent := api.JobPreemptedEvent{
		JobId:           "01gddx8ezywph2tbwfcvgpe5nn",
		JobSetId:        "test-set-a",
		Queue:           "queue-a",
		Created:         time.Now(),
		ClusterId:       "test-cluster",
		RunId:           "dde7325b-f1e9-43e6-8b38-f7a0ade07123",
		PreemptiveJobId: "01gddx9rjds05t37zd83379t9z",
		PreemptiveRunId: "db1da934-7366-449e-aed7-562e80730a35",
	}
	testEventMessage := api.EventMessage{Events: &api.EventMessage_Preempted{Preempted: &testEvent}}

	expectedPreemptedJobId, err := armadaevents.ProtoUuidFromUlidString(testEvent.JobId)
	assert.NoError(t, err)
	assert.NotEmpty(t, expectedPreemptedJobId)
	expectedPreemptedRunId, err := armadaevents.ProtoUuidFromUuidString(testEvent.RunId)
	assert.NoError(t, err)
	assert.NotEmpty(t, expectedPreemptedRunId)

	expectedPreemptiveJobId, err := armadaevents.ProtoUuidFromUlidString(testEvent.PreemptiveJobId)
	assert.NoError(t, err)
	assert.NotEmpty(t, expectedPreemptiveJobId)
	expectedPreemptiveRunId, err := armadaevents.ProtoUuidFromUuidString(testEvent.PreemptiveRunId)
	assert.NoError(t, err)
	assert.NotEmpty(t, expectedPreemptiveRunId)

	converted, err := EventSequenceFromApiEvent(&testEventMessage)

	assert.NoError(t, err)
	assert.Len(t, converted.Events, 1)
	assert.IsType(t, converted.Events[0].Event, &armadaevents.EventSequence_Event_JobRunPreempted{})

	evtSeqPreempted := converted.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunPreempted)
	assert.Equal(t, converted.JobSetName, testEvent.JobSetId)
	assert.Equal(t, converted.Queue, testEvent.Queue)
	assert.Equal(t, evtSeqPreempted.JobRunPreempted.PreemptedJobId, expectedPreemptedJobId)
	assert.Equal(t, evtSeqPreempted.JobRunPreempted.PreemptedRunId, expectedPreemptedRunId)
	assert.Equal(t, evtSeqPreempted.JobRunPreempted.PreemptiveJobId, expectedPreemptiveJobId)
	assert.Equal(t, evtSeqPreempted.JobRunPreempted.PreemptiveRunId, expectedPreemptiveRunId)
}

func TestEventSequenceFromApiEvent_Failed(t *testing.T) {
	testEvent := api.JobFailedEvent{
		JobId:        "01gddx8ezywph2tbwfcvgpe5nn",
		JobSetId:     "test-set-a",
		Queue:        "queue-a",
		Created:      time.Now(),
		ClusterId:    "test-cluster",
		Reason:       "some reason",
		KubernetesId: uuid.New().String(),
		NodeName:     "test-node",
		PodNumber:    1,
		PodName:      "test-pod",
		PodNamespace: "test-namespace",
		ContainerStatuses: []*api.ContainerStatus{
			{
				Name:     "container-1",
				ExitCode: 1,
				Message:  "test message",
				Reason:   "test reason",
				Cause:    api.Cause_OOM,
			},
		},
		Cause: api.Cause_DeadlineExceeded,
	}
	testEventMessage := api.EventMessage{Events: &api.EventMessage_Failed{Failed: &testEvent}}

	converted, err := EventSequenceFromApiEvent(&testEventMessage)

	require.NoError(t, err)
	require.Len(t, converted.Events, 2)

	expectedRunId, err := armadaevents.ProtoUuidFromUuidString(testEvent.KubernetesId)
	require.NoError(t, err)
	expectedJobId, err := armadaevents.ProtoUuidFromUlidString(testEvent.JobId)
	require.NoError(t, err)

	expectedErrors := []*armadaevents.Error{
		{
			Terminal: true,
			Reason: &armadaevents.Error_PodError{
				PodError: &armadaevents.PodError{
					ObjectMeta: &armadaevents.ObjectMeta{
						ExecutorId:   testEvent.ClusterId,
						Namespace:    testEvent.PodNamespace,
						Name:         testEvent.PodName,
						KubernetesId: testEvent.KubernetesId,
					},
					Message:          testEvent.Reason,
					NodeName:         testEvent.NodeName,
					PodNumber:        testEvent.PodNumber,
					KubernetesReason: armadaevents.KubernetesReason_DeadlineExceeded,
					ContainerErrors: []*armadaevents.ContainerError{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								ExecutorId:   testEvent.ClusterId,
								Namespace:    testEvent.PodNamespace,
								Name:         testEvent.ContainerStatuses[0].Name,
								KubernetesId: "", // only the id of the pod is stored in the failed message
							},
							ExitCode:         testEvent.ContainerStatuses[0].ExitCode,
							Message:          testEvent.ContainerStatuses[0].Message,
							Reason:           testEvent.ContainerStatuses[0].Reason,
							KubernetesReason: armadaevents.KubernetesReason_OOM,
						},
					},
				},
			},
		},
	}
	expectedEvents := []*armadaevents.EventSequence_Event{
		{
			Created: &testEvent.Created,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId:  expectedRunId,
					JobId:  expectedJobId,
					Errors: expectedErrors,
				},
			},
		},
		{
			Created: &testEvent.Created,
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId:  expectedJobId,
					Errors: expectedErrors,
				},
			},
		},
	}
	assert.Equal(t, expectedEvents, converted.Events)
}

func TestConvertJobSinglePodSpec(t *testing.T) {
	expected := testJob(false)

	ingressConfig := &configuration.IngressConfiguration{
		HostnameSuffix: "HostnameSuffix",
		CertNameSuffix: "CertNameSuffix",
		Annotations:    map[string]string{"ingress_annotation_1": "ingress_annotation_1", "ingress_annotation_2": "ingress_annotation_2"},
	}
	err := PopulateK8sServicesIngresses(expected, ingressConfig)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}

	// After converting to K8s objects, we don't need the API-specific objects.
	expected.Services = nil
	expected.Ingress = nil

	logJob, err := LogSubmitJobFromApiJob(expected)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}

	actual, err := ApiJobFromLogSubmitJob(
		expected.Owner,
		expected.QueueOwnershipUserGroups,
		expected.Queue,
		expected.JobSetId,
		expected.Created,
		logJob,
	)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}

	assert.Equal(t, expected, actual)
}

func TestConvertJobMultiplePodSpecs(t *testing.T) {
	expected := testJob(true)

	ingressConfig := &configuration.IngressConfiguration{
		HostnameSuffix: "HostnameSuffix",
		CertNameSuffix: "CertNameSuffix",
		Annotations:    map[string]string{"ingress_annotation_1": "ingress_annotation_1", "ingress_annotation_2": "ingress_annotation_2"},
	}
	err := PopulateK8sServicesIngresses(expected, ingressConfig)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}

	// After converting to K8s objects, we don't need the API-specific objects.
	expected.Services = nil
	expected.Ingress = nil

	logJob, err := LogSubmitJobFromApiJob(expected)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}

	actual, err := ApiJobFromLogSubmitJob(
		expected.Owner,
		expected.QueueOwnershipUserGroups,
		expected.Queue,
		expected.JobSetId,
		expected.Created,
		logJob,
	)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}

	assert.Equal(t, expected, actual)
}

func testJob(multiplePodSpecs bool) *api.Job {
	var mainPodSpec *v1.PodSpec
	var podSpec *v1.PodSpec
	var podSpecs []*v1.PodSpec
	if multiplePodSpecs {
		podSpecs = []*v1.PodSpec{testPodSpec("podSpec1"), testPodSpec("podSpec2")}
		mainPodSpec = podSpecs[0]
	} else {
		podSpec = testPodSpec("podSpec")
		mainPodSpec = podSpec
	}
	return &api.Job{
		Id:          util.NewULID(),
		ClientId:    "clientId",
		JobSetId:    "jobSet",
		Queue:       "queue",
		Namespace:   "namespace",
		Labels:      map[string]string{"label_1": "label_1", "label_2": "label_2"},
		Annotations: map[string]string{"annotation_1": "annotation_1", "annotation_2": "annotation_2"},
		// Deprecated and hence not part of the tests.
		// RequiredNodeLabels:       map[string]string{},
		Owner:                          "owner",
		QueueOwnershipUserGroups:       []string{"group1, group2"},
		Priority:                       1,
		PodSpec:                        podSpec,
		PodSpecs:                       podSpecs,
		SchedulingResourceRequirements: api.SchedulingResourceRequirementsFromPodSpec(mainPodSpec),
		Created:                        time.Now(),
		Ingress: []*api.IngressConfig{
			{
				Type:       api.IngressType_Ingress,
				Ports:      []uint32{5000},
				TlsEnabled: true,
			},
		},
		Services: []*api.ServiceConfig{
			{
				Type:  api.ServiceType_NodePort,
				Ports: []uint32{6000},
			},
			{
				Type:  api.ServiceType_Headless,
				Ports: []uint32{7000},
			},
		},
		K8SIngress: nil,
		K8SService: nil,
	}
}

func testPodSpec(name string) *v1.PodSpec {
	return &v1.PodSpec{
		Containers:   []v1.Container{testContainer(name + "_" + "container1"), testContainer(name + "_" + "container2")},
		NodeSelector: map[string]string{"nodeselector": "nodeselector_value"},
		Tolerations: []v1.Toleration{
			{
				Key:      "example.com/default_toleration_1",
				Value:    "value_1",
				Operator: v1.TolerationOpExists,
				Effect:   v1.TaintEffectNoSchedule,
			},
			{
				Key:      "example.com/default_toleration_2",
				Value:    "value_2",
				Operator: v1.TolerationOpEqual,
				Effect:   v1.TaintEffectNoSchedule,
			},
		},
	}
}

func testContainer(name string) v1.Container {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	return v1.Container{
		Name:    name,
		Image:   "alpine:3.18.3",
		Command: []string{"cmd1", "cmd2"},
		Args:    []string{"sleep", "5s"},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
			Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
		},
		Ports: []v1.ContainerPort{
			{
				ContainerPort: 5000,
				Protocol:      v1.ProtocolTCP,
				Name:          "port5000",
			},
			{
				ContainerPort: 6000,
				Protocol:      v1.ProtocolTCP,
				Name:          "port6000",
			},
			{
				ContainerPort: 7000,
				Protocol:      v1.ProtocolTCP,
				Name:          "port7000",
			},
		},
	}
}

func TestCompactSequences_Basic(t *testing.T) {
	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue2",
			UserId:     "userId2",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue2",
			UserId:     "userId2",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
	}

	actual := CompactEventSequences(sequences)
	assert.Equal(t, expected, actual)
}

func TestCompactSequences_JobSetOrder(t *testing.T) {
	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2", "group3"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_ReprioritiseJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2", "group3"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_ReprioritiseJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	actual := CompactEventSequences(sequences)
	assert.Equal(t, expected, actual)
}

func TestCompactSequences_Groups(t *testing.T) {
	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     nil,
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     nil,
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	actual := CompactEventSequences(sequences)
	assert.Equal(t, expected, actual)
}

func TestLimitSequenceByteSize(t *testing.T) {
	sequence := &armadaevents.EventSequence{
		Queue:      "queue1",
		UserId:     "userId1",
		JobSetName: "jobSetName1",
		Groups:     []string{"group1", "group2"},
		Events:     nil,
	}

	// 10 events, each of size 10 bytes + a little more.
	// At the time of writing, each event is 14 bytes and the sequence with no event is of size 46 bytes.
	numEvents := 3
	for i := 0; i < numEvents; i++ {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: &armadaevents.SubmitJob{
					DeduplicationId: "1234567890",
				},
			},
		})
	}

	actual, err := LimitSequenceByteSize(sequence, 1000, true)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []*armadaevents.EventSequence{sequence}, actual)

	_, err = LimitSequenceByteSize(sequence, 1, true)
	assert.Error(t, err)

	_, err = LimitSequenceByteSize(sequence, 1, false)
	assert.NoError(t, err)
	assert.Equal(t, []*armadaevents.EventSequence{sequence}, actual)

	expected := make([]*armadaevents.EventSequence, numEvents)
	for i := 0; i < numEvents; i++ {
		expected[i] = &armadaevents.EventSequence{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{
					Event: &armadaevents.EventSequence_Event_SubmitJob{
						SubmitJob: &armadaevents.SubmitJob{
							DeduplicationId: "1234567890",
						},
					},
				},
			},
		}
	}
	actual, err = LimitSequenceByteSize(sequence, 60, true)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, expected, actual)
}

func TestLimitSequencesByteSize(t *testing.T) {
	numSequences := 3
	numEvents := 3
	sequences := make([]*armadaevents.EventSequence, 0)
	for i := 0; i < numEvents; i++ {
		sequence := &armadaevents.EventSequence{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events:     nil,
		}

		// 10 events, each of size 10 bytes + a little more.
		// At the time of writing, each event is 14 bytes and the sequence with no event is of size 46 bytes.
		for i := 0; i < numEvents; i++ {
			sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
				Event: &armadaevents.EventSequence_Event_SubmitJob{
					SubmitJob: &armadaevents.SubmitJob{
						DeduplicationId: "1234567890",
					},
				},
			})
		}

		sequences = append(sequences, sequence)
	}

	actual, err := LimitSequencesByteSize(sequences, 60, true)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, numSequences*numEvents, len(actual))
}

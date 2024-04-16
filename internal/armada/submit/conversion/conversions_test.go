package conversion

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/internal/armada/submit/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var defaultIngressPathType = networking.PathType("Prefix")

func TestSubmitJobFromApiRequest(t *testing.T) {
	tests := map[string]struct {
		jobReq            *api.JobSubmitRequestItem
		expectedSubmitJob *armadaevents.SubmitJob
	}{
		"No Services or ingresses": {
			jobReq:            testfixtures.JobSubmitRequestItem(1),
			expectedSubmitJob: testfixtures.SubmitJob(1),
		},
		"NodePort Service": {
			jobReq: jobSubmitRequestItemWithServices([]*api.ServiceConfig{
				{
					Type:  api.ServiceType_NodePort,
					Ports: []uint32{8080},
				},
			}),
			expectedSubmitJob: SubmitJobMsgWithK8sObjects([]*armadaevents.KubernetesObject{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						Namespace: testfixtures.DefaultNamespace,
						Name:      "armada-00000000000000000000000001-0-service-0",
						Annotations: map[string]string{
							"armada_jobset_id": testfixtures.DefaultJobset,
							"armada_owner":     testfixtures.DefaultOwner,
						},
						Labels: map[string]string{
							"armada_job_id":   "00000000000000000000000001",
							"armada_queue_id": testfixtures.DefaultQueue.Name,
						},
					},
					Object: &armadaevents.KubernetesObject_Service{
						Service: &v1.ServiceSpec{
							Ports: []v1.ServicePort{
								{
									Name:     "testContainer-8080",
									Protocol: "TCP",
									Port:     8080,
								},
							},
							Selector: map[string]string{
								"armada_job_id": "00000000000000000000000001",
							},
							ClusterIP: "",
							Type:      v1.ServiceTypeNodePort,
						},
					},
				},
			}),
		},
		"Headless Service": {
			jobReq: jobSubmitRequestItemWithServices([]*api.ServiceConfig{
				{
					Type:  api.ServiceType_Headless,
					Ports: []uint32{8080},
				},
			}),
			expectedSubmitJob: SubmitJobMsgWithK8sObjects([]*armadaevents.KubernetesObject{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						Namespace: testfixtures.DefaultNamespace,
						Name:      "armada-00000000000000000000000001-0-service-0",
						Annotations: map[string]string{
							"armada_jobset_id": testfixtures.DefaultJobset,
							"armada_owner":     testfixtures.DefaultOwner,
						},
						Labels: map[string]string{
							"armada_job_id":   "00000000000000000000000001",
							"armada_queue_id": testfixtures.DefaultQueue.Name,
						},
					},
					Object: &armadaevents.KubernetesObject_Service{
						Service: &v1.ServiceSpec{
							Ports: []v1.ServicePort{
								{
									Name:     "testContainer-8080",
									Protocol: "TCP",
									Port:     8080,
								},
							},
							Selector: map[string]string{
								"armada_job_id": "00000000000000000000000001",
							},
							ClusterIP: "None",
							Type:      v1.ServiceTypeClusterIP,
						},
					},
				},
			}),
		},
		"Ingress": {
			jobReq: jobSubmitRequestItemWithIngresses([]*api.IngressConfig{
				{
					Ports: []uint32{8080},
				},
			}),
			expectedSubmitJob: SubmitJobMsgWithK8sObjects([]*armadaevents.KubernetesObject{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						Namespace: testfixtures.DefaultNamespace,
						Name:      "armada-00000000000000000000000001-0-service-0",
						Annotations: map[string]string{
							"armada_jobset_id": testfixtures.DefaultJobset,
							"armada_owner":     testfixtures.DefaultOwner,
						},
						Labels: map[string]string{
							"armada_job_id":   "00000000000000000000000001",
							"armada_queue_id": testfixtures.DefaultQueue.Name,
						},
					},
					Object: &armadaevents.KubernetesObject_Service{
						Service: &v1.ServiceSpec{
							Ports: []v1.ServicePort{
								{
									Name:     "testContainer-8080",
									Protocol: "TCP",
									Port:     8080,
								},
							},
							Selector: map[string]string{
								"armada_job_id": "00000000000000000000000001",
							},
							ClusterIP: "None",
							Type:      v1.ServiceTypeClusterIP,
						},
					},
				},
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						Namespace: testfixtures.DefaultNamespace,
						Name:      "armada-00000000000000000000000001-0-ingress-1",
						Annotations: map[string]string{
							"armada_jobset_id": testfixtures.DefaultJobset,
							"armada_owner":     testfixtures.DefaultOwner,
						},
						Labels: map[string]string{
							"armada_job_id":   "00000000000000000000000001",
							"armada_queue_id": testfixtures.DefaultQueue.Name,
						},
					},
					Object: &armadaevents.KubernetesObject_Ingress{
						Ingress: &networking.IngressSpec{
							TLS: []networking.IngressTLS{},
							Rules: []networking.IngressRule{
								{
									Host: "testContainer-8080-armada-00000000000000000000000001-0.testNamespace.",
									IngressRuleValue: networking.IngressRuleValue{
										HTTP: &networking.HTTPIngressRuleValue{
											Paths: []networking.HTTPIngressPath{
												{
													Path:     "/",
													PathType: &defaultIngressPathType,
													Backend: networking.IngressBackend{
														Service: &networking.IngressServiceBackend{
															Name: "armada-00000000000000000000000001-0-service-0",
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
						},
					},
				},
			}),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual := SubmitJobFromApiRequest(
				tc.jobReq,
				testfixtures.DefaultSubmissionConfig(),
				testfixtures.DefaultJobset, testfixtures.DefaultQueue.Name, testfixtures.DefaultOwner,
				func() *armadaevents.Uuid {
					return testfixtures.TestUlid(1)
				},
			)
			assert.Equal(t, tc.expectedSubmitJob, actual)
		})
	}
}

func TestCreateIngressFromService(t *testing.T) {
	defaultServiceSpec := &v1.ServiceSpec{
		Ports: []v1.ServicePort{
			{
				Name:     "testContainer-8080",
				Protocol: "TCP",
				Port:     8080,
			},
		},
		Selector: map[string]string{
			"armada_job_id": "00000000000000000000000001",
		},
		ClusterIP: "None",
		Type:      v1.ServiceTypeClusterIP,
	}

	defaultIngressRule := networking.IngressRule{
		Host: "testContainer-8080-armada-00000000000000000000000001-0.testNamespace.",
		IngressRuleValue: networking.IngressRuleValue{
			HTTP: &networking.HTTPIngressRuleValue{
				Paths: []networking.HTTPIngressPath{
					{
						Path:     "/",
						PathType: &defaultIngressPathType,
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
	}

	tests := map[string]struct {
		ingressConfig               *api.IngressConfig
		submissionConfigAnnotations map[string]string
		expectedIngress             *armadaevents.KubernetesObject
	}{
		"ingress": {
			ingressConfig: &api.IngressConfig{},
			expectedIngress: &armadaevents.KubernetesObject{
				ObjectMeta: &armadaevents.ObjectMeta{
					Name:        "armada-00000000000000000000000001-0-ingress-1",
					Annotations: map[string]string{},
					Labels:      map[string]string{},
				},
				Object: &armadaevents.KubernetesObject_Ingress{
					Ingress: &networking.IngressSpec{
						TLS:   []networking.IngressTLS{},
						Rules: []networking.IngressRule{defaultIngressRule},
					},
				},
			},
		},
		"ingressWithDefaultTls": {
			ingressConfig: &api.IngressConfig{
				TlsEnabled: true,
			},
			expectedIngress: &armadaevents.KubernetesObject{
				ObjectMeta: &armadaevents.ObjectMeta{
					Name:        "armada-00000000000000000000000001-0-ingress-1",
					Annotations: map[string]string{},
					Labels:      map[string]string{},
				},
				Object: &armadaevents.KubernetesObject_Ingress{
					Ingress: &networking.IngressSpec{
						TLS: []networking.IngressTLS{
							{
								Hosts:      []string{"testContainer-8080-armada-00000000000000000000000001-0.testNamespace."},
								SecretName: "testNamespace-",
							},
						},
						Rules: []networking.IngressRule{defaultIngressRule},
					},
				},
			},
		},
		"ingressWithCustomTls": {
			ingressConfig: &api.IngressConfig{
				TlsEnabled: true,
				CertName:   "testCustomCert",
			},
			expectedIngress: &armadaevents.KubernetesObject{
				ObjectMeta: &armadaevents.ObjectMeta{
					Name:        "armada-00000000000000000000000001-0-ingress-1",
					Annotations: map[string]string{},
					Labels:      map[string]string{},
				},
				Object: &armadaevents.KubernetesObject_Ingress{
					Ingress: &networking.IngressSpec{
						TLS: []networking.IngressTLS{
							{
								Hosts:      []string{"testContainer-8080-armada-00000000000000000000000001-0.testNamespace."},
								SecretName: "testCustomCert",
							},
						},
						Rules: []networking.IngressRule{defaultIngressRule},
					},
				},
			},
		},
		"ingressWithCustomAnnotations": {
			ingressConfig: &api.IngressConfig{
				Annotations: map[string]string{"testCustomAnnotationKey": "testCustomAnnotationVal"},
			},
			expectedIngress: &armadaevents.KubernetesObject{
				ObjectMeta: &armadaevents.ObjectMeta{
					Name:        "armada-00000000000000000000000001-0-ingress-1",
					Annotations: map[string]string{"testCustomAnnotationKey": "testCustomAnnotationVal"},
					Labels:      map[string]string{},
				},
				Object: &armadaevents.KubernetesObject_Ingress{
					Ingress: &networking.IngressSpec{
						TLS:   []networking.IngressTLS{},
						Rules: []networking.IngressRule{defaultIngressRule},
					},
				},
			},
		},
		"ingressWithConfigAnnotations": {
			ingressConfig: &api.IngressConfig{
				Annotations: map[string]string{"testCustomAnnotationKey": "testCustomAnnotationVal"},
			},
			expectedIngress: &armadaevents.KubernetesObject{
				ObjectMeta: &armadaevents.ObjectMeta{
					Name:        "armada-00000000000000000000000001-0-ingress-1",
					Annotations: map[string]string{"testCustomAnnotationKey": "testCustomAnnotationVal"},
					Labels:      map[string]string{},
				},
				Object: &armadaevents.KubernetesObject_Ingress{
					Ingress: &networking.IngressSpec{
						TLS:   []networking.IngressTLS{},
						Rules: []networking.IngressRule{defaultIngressRule},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			generatedIngress := createIngressFromService(
				defaultServiceSpec,
				1,
				tc.ingressConfig,
				"testService",
				testfixtures.DefaultNamespace,
				"00000000000000000000000001")

			assert.Equal(t, tc.expectedIngress, generatedIngress)
		})
	}
}

func TestPriorityAsInt32(t *testing.T) {
	tests := map[string]struct {
		priority         float64
		expectedPriority uint32
	}{
		"zero": {
			priority:         0.0,
			expectedPriority: uint32(0),
		},
		"negative priority": {
			priority:         -1.0,
			expectedPriority: uint32(0),
		},
		"valid priority": {
			priority:         1.0,
			expectedPriority: uint32(1),
		},
		"valid priority with fraction": {
			priority:         1.5,
			expectedPriority: uint32(2),
		},
		"Above bounds": {
			priority:         float64(math.MaxUint32) + 100,
			expectedPriority: uint32(math.MaxUint32),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expectedPriority, priorityAsInt32(tc.priority))
		})
	}
}

func jobSubmitRequestItemWithServices(s []*api.ServiceConfig) *api.JobSubmitRequestItem {
	req := testfixtures.JobSubmitRequestItem(1)
	req.Services = s
	return req
}

func jobSubmitRequestItemWithIngresses(i []*api.IngressConfig) *api.JobSubmitRequestItem {
	req := testfixtures.JobSubmitRequestItem(1)
	req.Ingress = i
	return req
}

func SubmitJobMsgWithK8sObjects(s []*armadaevents.KubernetesObject) *armadaevents.SubmitJob {
	submitMsg := testfixtures.SubmitJob(1)
	submitMsg.Objects = s
	return submitMsg
}

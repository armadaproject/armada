package job

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientTesting "k8s.io/client-go/testing"

	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
)

func setupSubmitTest() (*SubmitService, *fake.Clientset) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	client := fake.NewSimpleClientset()
	clientProvider := &fakeClientProvider{FakeClient: client}
	clusterContext := executorContext.NewClusterContext(
		configuration.ApplicationConfiguration{ClusterId: "test-cluster", Pool: "pool", DeleteConcurrencyLimit: 1},
		2*time.Minute,
		clientProvider,
		5*time.Minute,
	)
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{})
	return submitter, client
}

type fakeClientProvider struct {
	FakeClient *fake.Clientset
}

func (p *fakeClientProvider) ClientForUser(user string, groups []string) (kubernetes.Interface, error) {
	return p.FakeClient, nil
}

func (p *fakeClientProvider) Client() kubernetes.Interface {
	return p.FakeClient
}

func (p *fakeClientProvider) ClientConfig() *rest.Config {
	return nil
}

func TestCreateServiceWithRetry(t *testing.T) {
	tests := map[string]struct {
		setupReactors   func(client *fake.Clientset, serviceName string)
		expectError     bool
		errorContains   string
		expectedActions int
	}{
		"success on first attempt": {
			setupReactors:   func(client *fake.Clientset, serviceName string) {},
			expectError:     false,
			expectedActions: 2, // 1 delete + 1 create
		},
		"succeeds after AlreadyExists retry": {
			setupReactors: func(client *fake.Clientset, serviceName string) {
				var createCount int32
				client.Fake.PrependReactor("create", "services", func(action clientTesting.Action) (bool, runtime.Object, error) {
					if atomic.AddInt32(&createCount, 1) == 1 {
						return true, nil, k8s_errors.NewAlreadyExists(v1.Resource("services"), serviceName)
					}
					return false, nil, nil
				})
			},
			expectError:     false,
			expectedActions: 4, // delete + create(fail) + delete + create(success)
		},
		"fails after max retries exceeded": {
			setupReactors: func(client *fake.Clientset, serviceName string) {
				client.Fake.PrependReactor("create", "services", func(action clientTesting.Action) (bool, runtime.Object, error) {
					return true, nil, k8s_errors.NewAlreadyExists(v1.Resource("services"), serviceName)
				})
			},
			expectError:     true,
			errorContains:   "still exists after 5 delete attempts",
			expectedActions: 10, // 5 attempts x (delete + create)
		},
		"fails immediately on delete error": {
			setupReactors: func(client *fake.Clientset, serviceName string) {
				client.Fake.PrependReactor("delete", "services", func(action clientTesting.Action) (bool, runtime.Object, error) {
					return true, nil, k8s_errors.NewForbidden(v1.Resource("services"), serviceName, nil)
				})
			},
			expectError:     true,
			errorContains:   "failed to delete existing service",
			expectedActions: 1, // only delete attempted
		},
		"fails immediately on non-AlreadyExists create error": {
			setupReactors: func(client *fake.Clientset, serviceName string) {
				client.Fake.PrependReactor("create", "services", func(action clientTesting.Action) (bool, runtime.Object, error) {
					return true, nil, k8s_errors.NewForbidden(v1.Resource("services"), serviceName, nil)
				})
			},
			expectError:     true,
			expectedActions: 2, // 1 delete + 1 create (no retry)
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			submitter, client := setupSubmitTest()

			service := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
			}

			tc.setupReactors(client, service.Name)
			client.Fake.ClearActions()

			err := submitter.createServiceWithRetry(service)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}

			actions := filterActions(client.Fake.Actions(), "services")
			assert.Equal(t, tc.expectedActions, len(actions))
		})
	}
}

func TestCreateIngressWithRetry(t *testing.T) {
	tests := map[string]struct {
		setupReactors   func(client *fake.Clientset, ingressName string)
		expectError     bool
		errorContains   string
		expectedActions int
	}{
		"success on first attempt": {
			setupReactors:   func(client *fake.Clientset, ingressName string) {},
			expectError:     false,
			expectedActions: 2, // 1 delete + 1 create
		},
		"succeeds after AlreadyExists retry": {
			setupReactors: func(client *fake.Clientset, ingressName string) {
				var createCount int32
				client.Fake.PrependReactor("create", "ingresses", func(action clientTesting.Action) (bool, runtime.Object, error) {
					if atomic.AddInt32(&createCount, 1) == 1 {
						return true, nil, k8s_errors.NewAlreadyExists(networking.Resource("ingresses"), ingressName)
					}
					return false, nil, nil
				})
			},
			expectError:     false,
			expectedActions: 4, // delete + create(fail) + delete + create(success)
		},
		"fails after max retries exceeded": {
			setupReactors: func(client *fake.Clientset, ingressName string) {
				client.Fake.PrependReactor("create", "ingresses", func(action clientTesting.Action) (bool, runtime.Object, error) {
					return true, nil, k8s_errors.NewAlreadyExists(networking.Resource("ingresses"), ingressName)
				})
			},
			expectError:     true,
			errorContains:   "still exists after 5 delete attempts",
			expectedActions: 10, // 5 attempts x (delete + create)
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			submitter, client := setupSubmitTest()

			ingress := &networking.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-ingress",
					Namespace: "default",
				},
			}

			tc.setupReactors(client, ingress.Name)
			client.Fake.ClearActions()

			err := submitter.createIngressWithRetry(ingress)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}

			actions := filterActions(client.Fake.Actions(), "ingresses")
			assert.Equal(t, tc.expectedActions, len(actions))
		})
	}
}

// filterActions returns only actions matching the given resource
func filterActions(actions []clientTesting.Action, resource string) []clientTesting.Action {
	var filtered []clientTesting.Action
	for _, action := range actions {
		if action.GetResource().Resource == resource {
			filtered = append(filtered, action)
		}
	}
	return filtered
}

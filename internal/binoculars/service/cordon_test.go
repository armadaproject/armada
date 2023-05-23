package service

import (
	"context"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/pkg/api/binoculars"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

var (
	defaultCordonConfig = configuration.CordonConfiguration{
		AdditionalLabels: map[string]string{},
	}
	defaultNode = &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Node1",
		},
	}
)

func TestCordonNode(t *testing.T) {
	cordonService, client := setupTest(t, defaultCordonConfig, FakePermissionChecker{ReturnValue: true})

	err := cordonService.CordonNode(context.Background(), &binoculars.CordonRequest{
		NodeName: "Node1",
	})
	assert.Nil(t, err)

	actions := client.Fake.Actions()
	assert.Len(t, actions, 1)

	node, err := client.CoreV1().Nodes().Get(context.Background(), defaultNode.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, node.Spec.Unschedulable, true)
}

func TestCordonNode_InvalidNodeName(t *testing.T) {
	cordonService, _ := setupTest(t, defaultCordonConfig, FakePermissionChecker{ReturnValue: true})

	err := cordonService.CordonNode(context.Background(), &binoculars.CordonRequest{
		NodeName: "non-existent-node",
	})

	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
}

func TestCordonNode_Unauthenticated(t *testing.T) {
	cordonService, _ := setupTest(t, defaultCordonConfig, FakePermissionChecker{ReturnValue: false})
	err := cordonService.CordonNode(context.Background(), &binoculars.CordonRequest{
		NodeName: defaultNode.Name,
	})

	assert.Error(t, err)
	statusError, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, statusError.Code(), codes.PermissionDenied)
}

func TestCordonNode_AddsAdditionalLabelsOnCordon(t *testing.T) {
	cordonConfig := configuration.CordonConfiguration{
		AdditionalLabels: map[string]string{
			"armadaproject.io/cordon-reason": "test",
		},
	}
	cordonService, client := setupTest(t, cordonConfig, FakePermissionChecker{ReturnValue: true})

	err := cordonService.CordonNode(context.Background(), &binoculars.CordonRequest{
		NodeName: defaultNode.Name,
	})
	assert.Nil(t, err)

	actions := client.Fake.Actions()
	assert.Len(t, actions, 1)

	time.Sleep(time.Second * 2)

	node, err := client.CoreV1().Nodes().Get(context.Background(), defaultNode.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, node.Spec.Unschedulable, true)
	assert.Equal(t, node.Labels, cordonConfig.AdditionalLabels)
}

func TestCordonNode_TemplatesLabels(t *testing.T) {

}

func setupTest(t *testing.T, config configuration.CordonConfiguration, permissionChecker authorization.PermissionChecker) (CordonService, *fake.Clientset) {
	client := fake.NewSimpleClientset()
	clientProvider := &FakeClientProvider{FakeClient: client}

	_, err := client.CoreV1().Nodes().Create(context.Background(), defaultNode, metav1.CreateOptions{})
	require.NoError(t, err)
	client.Fake.ClearActions()

	cordonService := NewKubernetesCordonService(config, permissionChecker, clientProvider)
	return cordonService, client
}

type FakePermissionChecker struct {
	ReturnValue bool
}

func (c FakePermissionChecker) UserOwns(ctx context.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return c.ReturnValue, []string{}
}

func (c FakePermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	return c.ReturnValue
}

type FakeClientProvider struct {
	FakeClient *fake.Clientset
	users      []string
}

func (p *FakeClientProvider) ClientForUser(user string, groups []string) (kubernetes.Interface, error) {
	p.users = append(p.users, user)
	return p.FakeClient, nil
}

func (p *FakeClientProvider) Client() kubernetes.Interface {
	return p.FakeClient
}

func (p *FakeClientProvider) ClientConfig() *rest.Config {
	return nil
}

package service

import (
	gocontext "context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"testing"

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
	clientTesting "k8s.io/client-go/testing"

	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/pkg/api/binoculars"
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
	principal := authorization.NewStaticPrincipal("principle", []string{})
	tests := map[string]struct {
		additionalLabels map[string]string
		expectedLabels   map[string]string
		expectedPatch    *nodePatch
	}{
		"basic cordon": {
			expectedPatch: &nodePatch{Spec: v1.NodeSpec{Unschedulable: true}},
		},
		"with additional labels": {
			additionalLabels: map[string]string{
				"armadaproject.io/cordon-reason": "test",
			},
			expectedLabels: map[string]string{
				"armadaproject.io/cordon-reason": "test",
			},
			expectedPatch: &nodePatch{
				Spec:     v1.NodeSpec{Unschedulable: true},
				MetaData: metav1.ObjectMeta{Labels: map[string]string{"armadaproject.io/cordon-reason": "test"}},
			},
		},
		"with templated labels": {
			additionalLabels: map[string]string{
				"armadaproject.io/<user>": "<user>",
			},
			expectedLabels: map[string]string{
				fmt.Sprintf("armadaproject.io/%s", principal.GetName()): principal.GetName(),
			},
			expectedPatch: &nodePatch{
				Spec:     v1.NodeSpec{Unschedulable: true},
				MetaData: metav1.ObjectMeta{Labels: map[string]string{fmt.Sprintf("armadaproject.io/%s", principal.GetName()): principal.GetName()}},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cordonConfig := configuration.CordonConfiguration{
				AdditionalLabels: tc.additionalLabels,
			}
			cordonService, client := setupTest(t, cordonConfig, FakePermissionChecker{ReturnValue: true})

			ctx := authorization.WithPrincipal(gocontext.Background(), principal)
			err := cordonService.CordonNode(context.New(ctx, logrus.NewEntry(logrus.New())), &binoculars.CordonRequest{
				NodeName: defaultNode.Name,
			})
			assert.Nil(t, err)

			// Assert the correct patch happened
			actions := client.Fake.Actions()
			assert.Len(t, actions, 1)
			patchAction, ok := client.Fake.Actions()[0].(clientTesting.PatchAction)
			assert.True(t, ok)
			assert.Equal(t, patchAction.GetName(), defaultNode.Name)
			patch := &nodePatch{}
			err = json.Unmarshal(patchAction.GetPatch(), patch)
			assert.NoError(t, err)
			assert.Equal(t, patch, tc.expectedPatch)

			// Assert resulting node is in expected state
			node, err := client.CoreV1().Nodes().Get(context.Background(), defaultNode.Name, metav1.GetOptions{})
			assert.Nil(t, err)
			assert.Equal(t, node.Spec.Unschedulable, true)
			assert.Equal(t, node.Labels, tc.expectedLabels)
		})
	}
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

func (c FakePermissionChecker) UserOwns(ctx gocontext.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return c.ReturnValue, []string{}
}

func (c FakePermissionChecker) UserHasPermission(ctx gocontext.Context, perm permission.Permission) bool {
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

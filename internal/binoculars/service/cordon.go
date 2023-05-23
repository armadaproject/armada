package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/cluster"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

const userTemplate = "<user>"

type CordonService interface {
	CordonNode(ctx context.Context, request *binoculars.CordonRequest) error
}

type KubernetesCordonService struct {
	clientProvider    cluster.KubernetesClientProvider
	permissionChecker authorization.PermissionChecker
	config            configuration.CordonConfiguration
}

func NewKubernetesCordonService(
	cordonConfig configuration.CordonConfiguration,
	permissionsChecker authorization.PermissionChecker,
	clientProvider cluster.KubernetesClientProvider) *KubernetesCordonService {
	return &KubernetesCordonService{
		clientProvider:    clientProvider,
		permissionChecker: permissionsChecker,
		config:            cordonConfig,
	}
}

func (c *KubernetesCordonService) CordonNode(ctx context.Context, request *binoculars.CordonRequest) error {
	err := checkPermission(c.permissionChecker, ctx, permissions.CordonNodes)
	if err != nil {
		return status.Errorf(codes.PermissionDenied, err.Error())
	}

	additionalLabels := templateLabels(c.config.AdditionalLabels, authorization.GetPrincipal(ctx).GetName())
	patch := createCordonPatch(additionalLabels)
	patchBytes, err := GetPatchBytes(patch)

	client := c.clientProvider.Client()
	_, err = client.CoreV1().Nodes().Patch(ctx, request.NodeName, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	return err
}

func templateLabels(labels map[string]string, user string) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		templatedKey := strings.ReplaceAll(key, userTemplate, user)
		templatedValue := strings.ReplaceAll(value, userTemplate, user)
		result[templatedKey] = templatedValue
	}
	return result
}

func createCordonPatch(labels map[string]string) []*NodePatch {
	patch := []*NodePatch{}

	patch = append(patch, &NodePatch{
		Op:    "replace",
		Path:  "/spec/unschedulable",
		Value: true,
	})

	for key, value := range labels {
		patch = append(patch, &NodePatch{
			Op:    "replace",
			Path:  fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(key, "/", "~1")),
			Value: value,
		})
	}

	return patch
}

func GetPatchBytes(patchData []*NodePatch) ([]byte, error) {
	return json.Marshal(patchData)
}

type NodePatch struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		return fmt.Errorf("user %s does not have permission %s", authorization.GetPrincipal(ctx), permission)
	}
	return nil
}

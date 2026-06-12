package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/cluster"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

const userTemplate = "<user>"

type CordonService interface {
	CordonNode(ctx *armadacontext.Context, request *binoculars.CordonRequest) error
}

type KubernetesCordonService struct {
	clientProvider    cluster.KubernetesClientProvider
	permissionChecker auth.PermissionChecker
	config            configuration.CordonConfiguration
}

func NewKubernetesCordonService(
	cordonConfig configuration.CordonConfiguration,
	permissionsChecker auth.PermissionChecker,
	clientProvider cluster.KubernetesClientProvider,
) *KubernetesCordonService {
	return &KubernetesCordonService{
		clientProvider:    clientProvider,
		permissionChecker: permissionsChecker,
		config:            cordonConfig,
	}
}

func (c *KubernetesCordonService) CordonNode(ctx *armadacontext.Context, request *binoculars.CordonRequest) error {
	err := checkPermission(c.permissionChecker, ctx, permissions.CordonNodes)
	if err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	user := auth.GetPrincipal(ctx).GetName()
	additionalLabels := templateLabels(c.config.AdditionalLabels, user)
	patch := createCordonPatch(additionalLabels)
	patchBytes, err := GetPatchBytes(patch)
	if err != nil {
		return fmt.Errorf("failed to build cordon patch for node %s: %w", request.NodeName, err)
	}

	client := c.clientProvider.Client()
	_, err = client.CoreV1().Nodes().Patch(ctx, request.NodeName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to cordon node %s: %w", request.NodeName, err)
	}
	log.WithFields(map[string]any{"node": request.NodeName, "user": user}).Info("cordoned node")
	return nil
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

type nodePatch struct {
	MetaData metav1.ObjectMeta `json:"metadata"`
	Spec     v1.NodeSpec       `json:"spec"`
}

func createCordonPatch(labels map[string]string) *nodePatch {
	patch := &nodePatch{}

	patch.Spec = v1.NodeSpec{
		Unschedulable: true,
	}

	patch.MetaData = metav1.ObjectMeta{
		Labels: labels,
	}
	return patch
}

func GetPatchBytes(patchData *nodePatch) ([]byte, error) {
	return json.Marshal(patchData)
}

func checkPermission(p auth.PermissionChecker, ctx *armadacontext.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		return fmt.Errorf("user %s does not have permission %s", auth.GetPrincipal(ctx).GetName(), permission)
	}
	return nil
}

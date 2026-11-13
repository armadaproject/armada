package context

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/executor/domain"
)

// Only these claims may be superseded and cleaned up by a later versioned
// conditional attempt. All other values represent intent that may have an
// unconditional DELETE in flight and must never become conditionally removable.
const conditionalDeletionPrefix = "conditional:"

func isConditionalDeletionMarker(marker string) bool {
	token, ok := strings.CutPrefix(marker, conditionalDeletionPrefix)
	if !ok {
		return false
	}
	id, _, _ := strings.Cut(token, " ")
	_, err := uuid.Parse(id)
	return err == nil
}

func (c *KubernetesClusterContext) claimDeletion(pod *v1.Pod, resourceVersion string, conditional bool) (*v1.Pod, error) {
	marker := uuid.NewString() + " " + time.Now().String()
	if conditional {
		marker = conditionalDeletionPrefix + marker
	}
	patch := &domain.Patch{MetaData: metav1.ObjectMeta{UID: pod.UID, ResourceVersion: resourceVersion, Annotations: map[string]string{domain.MarkedForDeletion: marker}}}
	data, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	return c.kubernetesClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
}

// A timeout, transport error or server failure may have committed the DELETE.
// Only explicit request rejection permits relinquishing reporting suppression.
func deletionDefinitelyRejected(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsMethodNotSupported(err)
}

func (c *KubernetesClusterContext) removeAbandonedDeletionMarker(claim *v1.Pod) error {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	for attempt := 0; attempt < 3; attempt++ {
		current, err := c.kubernetesClient.CoreV1().Pods(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.UID != claim.UID || current.DeletionTimestamp != nil || current.Annotations[domain.MarkedForDeletion] != claim.Annotations[domain.MarkedForDeletion] {
			return nil
		}
		// The version protects both the token and deletionTimestamp checks from
		// concurrent adoption/acceptance. Remove only this key, never all annotations.
		patch := map[string]interface{}{"metadata": map[string]interface{}{"uid": current.UID, "resourceVersion": current.ResourceVersion, "annotations": map[string]interface{}{domain.MarkedForDeletion: nil}}}
		data, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		_, err = c.kubernetesClient.CoreV1().Pods(claim.Namespace).Patch(ctx, claim.Name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if !apierrors.IsConflict(err) || attempt == 2 {
			return err
		}
	}
	return nil
}

package podwriter

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Validate enforces the narrow POC pod shape. Anything outside this shape is
// rejected rather than approximated, because the writer does not run admission.
func Validate(pod *v1.Pod) error {
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}
	if messages := validation.IsDNS1123Label(pod.Namespace); len(messages) > 0 {
		return fmt.Errorf("invalid namespace %q: %s", pod.Namespace, messages[0])
	}
	if messages := validation.IsDNS1123Label(pod.Name); len(messages) > 0 {
		return fmt.Errorf("invalid pod name %q: %s", pod.Name, messages[0])
	}
	if pod.DeletionTimestamp != nil {
		return fmt.Errorf("pod is being deleted")
	}
	if len(pod.OwnerReferences) > 0 {
		return fmt.Errorf("owner references are not supported")
	}
	if len(pod.Finalizers) > 0 {
		return fmt.Errorf("finalizers are not supported")
	}
	if len(pod.Spec.Containers) != 1 {
		return fmt.Errorf("exactly one regular container is required")
	}
	if len(pod.Spec.InitContainers) > 0 {
		return fmt.Errorf("init containers are not supported")
	}
	if len(pod.Spec.EphemeralContainers) > 0 {
		return fmt.Errorf("ephemeral containers are not supported")
	}
	if pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC {
		return fmt.Errorf("host namespaces are not supported")
	}
	if pod.Spec.ServiceAccountName != "" && pod.Spec.ServiceAccountName != defaultServiceAccount {
		return fmt.Errorf("only the default service account is supported")
	}
	if pod.Spec.PreemptionPolicy != nil {
		return fmt.Errorf("preemption policy is not supported")
	}
	if pod.Spec.RuntimeClassName != nil {
		return fmt.Errorf("runtime classes are not supported")
	}
	if len(pod.Spec.SchedulingGates) > 0 {
		return fmt.Errorf("scheduling gates are not supported")
	}
	if pod.Spec.Affinity != nil {
		return fmt.Errorf("affinity is not supported")
	}
	if pod.Spec.Resources != nil || pod.Spec.Overhead != nil {
		return fmt.Errorf("pod-level resources and overhead are not supported")
	}

	container := &pod.Spec.Containers[0]
	if container.Name == "" {
		return fmt.Errorf("container name is empty")
	}
	if container.Image == "" {
		return fmt.Errorf("container image is empty")
	}
	if container.SecurityContext != nil {
		if container.SecurityContext.Privileged != nil && *container.SecurityContext.Privileged {
			return fmt.Errorf("privileged containers are not supported")
		}
		if container.SecurityContext.AllowPrivilegeEscalation != nil && *container.SecurityContext.AllowPrivilegeEscalation {
			return fmt.Errorf("allowPrivilegeEscalation is not supported")
		}
		if container.SecurityContext.Capabilities != nil && len(container.SecurityContext.Capabilities.Add) > 0 {
			return fmt.Errorf("added capabilities are not supported")
		}
	}
	if err := validateResources(container.Resources); err != nil {
		return err
	}

	for i := range pod.Spec.Volumes {
		if !allowedVolume(&pod.Spec.Volumes[i]) {
			return fmt.Errorf("volume %q uses an unsupported volume type", pod.Spec.Volumes[i].Name)
		}
	}
	return nil
}

func validateResources(resources v1.ResourceRequirements) error {
	for _, name := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory} {
		request, hasRequest := resources.Requests[name]
		limit, hasLimit := resources.Limits[name]
		if !hasRequest || !hasLimit {
			return fmt.Errorf("%s request and limit are required", name)
		}
		if request.Cmp(resource.MustParse("0")) <= 0 || limit.Cmp(resource.MustParse("0")) <= 0 {
			return fmt.Errorf("%s request and limit must be positive", name)
		}
	}
	return nil
}

func allowedVolume(volume *v1.Volume) bool {
	count := 0
	if volume.EmptyDir != nil {
		count++
	}
	if volume.ConfigMap != nil {
		count++
	}
	if volume.Secret != nil {
		count++
	}
	if volume.PersistentVolumeClaim != nil {
		count++
	}
	return count == 1
}

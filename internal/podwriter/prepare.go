package podwriter

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/ptr"
)

const (
	defaultTerminationGracePeriodSeconds int64 = 30
	defaultSchedulerName                       = "default-scheduler"
	defaultServiceAccount                      = "default"
)

// PrepareOptions controls the few executor-specific values needed to make a
// pod look like one created by the API server.
type PrepareOptions struct {
	SchedulerName string
	NodeIdLabel   string
}

// Prepare applies the defaults required by the POC allowlist, initializes
// system metadata, sets the pod's node, and resets status. The API server
// normally does all of this inside its create strategy.
func Prepare(pod *v1.Pod, opts PrepareOptions) error {
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}
	if pod.Namespace == "" {
		return fmt.Errorf("pod namespace is empty")
	}
	if pod.Name == "" {
		return fmt.Errorf("pod name is empty")
	}

	pod.UID = uuid.NewUUID()
	pod.CreationTimestamp = metav1.Now()
	pod.ResourceVersion = ""
	pod.DeletionTimestamp = nil
	pod.DeletionGracePeriodSeconds = nil
	pod.Generation = 0
	pod.ManagedFields = nil
	pod.OwnerReferences = nil
	pod.Finalizers = nil

	spec := &pod.Spec
	spec.RestartPolicy = v1.RestartPolicyNever
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = v1.DNSClusterFirst
	}
	if spec.TerminationGracePeriodSeconds == nil {
		spec.TerminationGracePeriodSeconds = ptr.To(defaultTerminationGracePeriodSeconds)
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = opts.SchedulerName
		if spec.SchedulerName == "" {
			spec.SchedulerName = defaultSchedulerName
		}
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = &v1.PodSecurityContext{}
	}
	if spec.EnableServiceLinks == nil {
		spec.EnableServiceLinks = ptr.To(true)
	}
	if spec.ServiceAccountName == "" {
		spec.ServiceAccountName = defaultServiceAccount
	}

	if opts.NodeIdLabel != "" {
		if nodeName, ok := spec.NodeSelector[opts.NodeIdLabel]; ok && nodeName != "" {
			if spec.NodeName != "" && spec.NodeName != nodeName {
				return fmt.Errorf("pod nodeName %q conflicts with node selector %q", spec.NodeName, nodeName)
			}
			spec.NodeName = nodeName
			delete(spec.NodeSelector, opts.NodeIdLabel)
		}
	}

	for i := range spec.Containers {
		defaultContainer(&spec.Containers[i])
	}
	for i := range spec.InitContainers {
		defaultContainer(&spec.InitContainers[i])
	}
	for i := range spec.Volumes {
		defaultVolume(&spec.Volumes[i])
	}

	pod.Status = v1.PodStatus{
		Phase:    v1.PodPending,
		QOSClass: qosClass(pod),
	}
	return nil
}

func defaultContainer(c *v1.Container) {
	if c.ImagePullPolicy == "" {
		c.ImagePullPolicy = defaultImagePullPolicy(c.Image)
	}
	if c.TerminationMessagePath == "" {
		c.TerminationMessagePath = v1.TerminationMessagePathDefault
	}
	if c.TerminationMessagePolicy == "" {
		c.TerminationMessagePolicy = v1.TerminationMessageReadFile
	}
	if c.Resources.Limits != nil {
		if c.Resources.Requests == nil {
			c.Resources.Requests = v1.ResourceList{}
		}
		for name, limit := range c.Resources.Limits {
			if _, ok := c.Resources.Requests[name]; !ok {
				c.Resources.Requests[name] = limit.DeepCopy()
			}
		}
	}
	defaultProbe(c.LivenessProbe)
	defaultProbe(c.ReadinessProbe)
	defaultProbe(c.StartupProbe)
}

func defaultProbe(p *v1.Probe) {
	if p == nil {
		return
	}
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = 1
	}
	if p.PeriodSeconds == 0 {
		p.PeriodSeconds = 10
	}
	if p.SuccessThreshold == 0 {
		p.SuccessThreshold = 1
	}
	if p.FailureThreshold == 0 {
		p.FailureThreshold = 3
	}
}

func defaultVolume(v *v1.Volume) {
	mode := int32(420)
	switch {
	case v.Secret != nil && v.Secret.DefaultMode == nil:
		v.Secret.DefaultMode = &mode
	case v.ConfigMap != nil && v.ConfigMap.DefaultMode == nil:
		v.ConfigMap.DefaultMode = &mode
	case v.DownwardAPI != nil && v.DownwardAPI.DefaultMode == nil:
		v.DownwardAPI.DefaultMode = &mode
	case v.Projected != nil && v.Projected.DefaultMode == nil:
		v.Projected.DefaultMode = &mode
	}
}

func defaultImagePullPolicy(image string) v1.PullPolicy {
	if strings.Contains(image, "@") {
		return v1.PullIfNotPresent
	}
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon <= lastSlash {
		return v1.PullAlways
	}
	if image[lastColon+1:] == "latest" {
		return v1.PullAlways
	}
	return v1.PullIfNotPresent
}

func qosClass(pod *v1.Pod) v1.PodQOSClass {
	guaranteed := true
	bestEffort := true
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		cpuRequest, hasCPURequest := c.Resources.Requests[v1.ResourceCPU]
		memoryRequest, hasMemoryRequest := c.Resources.Requests[v1.ResourceMemory]
		cpuLimit, hasCPULimit := c.Resources.Limits[v1.ResourceCPU]
		memoryLimit, hasMemoryLimit := c.Resources.Limits[v1.ResourceMemory]
		if !hasCPURequest && !hasMemoryRequest && !hasCPULimit && !hasMemoryLimit {
			continue
		}
		bestEffort = false
		if !hasCPURequest || !hasMemoryRequest || !hasCPULimit || !hasMemoryLimit {
			guaranteed = false
			continue
		}
		if cpuRequest.Cmp(cpuLimit) != 0 || memoryRequest.Cmp(memoryLimit) != 0 {
			guaranteed = false
		}
	}
	if bestEffort {
		return v1.PodQOSBestEffort
	}
	if guaranteed {
		return v1.PodQOSGuaranteed
	}
	return v1.PodQOSBurstable
}

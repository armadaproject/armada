package util

import (
	"github.com/G-Research/k8s-batch/internal/common"
	v1 "k8s.io/api/core/v1"
)

func CalculateTotalResource(nodes []*v1.Node) common.ComputeResources {
	totalResources := make(common.ComputeResources)
	for _, node := range nodes {
		nodeAllocatableResource := common.FromResourceList(node.Status.Allocatable)
		totalResources.Add(nodeAllocatableResource)
	}
	return totalResources
}

func CalculateTotalResourceLimit(pods []*v1.Pod) common.ComputeResources {
	totalResources := make(common.ComputeResources)
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			containerResourceLimit := common.FromResourceList(container.Resources.Limits)
			totalResources.Add(containerResourceLimit)
		}
		// Todo determine what to do about init containers? How does Kubernetes scheduler handle these
	}
	return totalResources
}

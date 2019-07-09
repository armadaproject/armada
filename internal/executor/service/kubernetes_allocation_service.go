package service

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/submitter"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers/core/v1"
)

type KubernetesAllocationService struct {
	PodLister listers.PodLister
	NodeLister listers.NodeLister
	JobSubmitter submitter.JobSubmitter
}

func (allocationService KubernetesAllocationService) FillInSpareClusterCapacity() {
	allNodes, err := allocationService.NodeLister.List(labels.Everything())
	if err != nil {
		fmt.Println("Error getting node information")
	}

	allPods, err := allocationService.PodLister.List(labels.Everything())
	if err != nil {
		fmt.Println("Error getting pod information")
	}
	// Todo Inefficient? We could monitor changes on nodes + pods and keep an internal map of where they are. However then we would be maintaining 2 internal maps (ours + informer)

	processingNodes := getAllAvailableProcessingNodes(allNodes)
	podsOnProcessingNodes := getAllPodsOnNodes(allPods, processingNodes);

	totalNodeCpu := calculateTotalCpu(processingNodes)
	totalNodeMemory := calculateTotalMemory(processingNodes)

	totalPodCpuLimit := calculateTotalCpuLimit(podsOnProcessingNodes)
	totalPodMemoryLimit := calculateTotalMemoryLimit(podsOnProcessingNodes)

	freeCpu := totalNodeCpu.DeepCopy()
	freeCpu.Sub(totalPodCpuLimit)

	freeMemory := totalNodeMemory.DeepCopy()
	freeMemory.Sub(totalPodMemoryLimit)

	//newJobs := jobRequest.RequestJobs(freeCpu, freeMemory)
	//for _, job := range newJobs {
	//	jobSubmitter.SubmitJob(job, "default")
	//}

}

func getAllAvailableProcessingNodes(nodes []*v1.Node) []*v1.Node {
	processingNodes := make([]*v1.Node, 0, len(nodes))

	for _, node := range nodes {
		if isAvailableProcessingNode(node) {
			processingNodes = append(processingNodes, node)
		}
	}

	return processingNodes
}

func isAvailableProcessingNode(node *v1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}

	noSchedule := false

	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule {
			noSchedule = true
			break
		}
	}

	if noSchedule {
		return false
	}

	return true
}

func getAllPodsOnNodes(pods []*v1.Pod, nodes []*v1.Node) []*v1.Pod {
	podsBelongingToNodes := make([]*v1.Pod, 0, len(pods))

	nodeMap := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeMap[node.Name] = node
	}

	for _, pod := range pods {
		if _, present := nodeMap[pod.Spec.NodeName]; present {
			podsBelongingToNodes = append(podsBelongingToNodes, pod)
		}
	}

	return podsBelongingToNodes
}

func calculateTotalCpu(nodes []*v1.Node) resource.Quantity {
	totalCpu := resource.Quantity{}
	for _, node := range nodes {
		nodeAllocatableCpu := node.Status.Allocatable.Cpu()
		totalCpu.Add(*nodeAllocatableCpu)
	}
	return totalCpu
}

func calculateTotalMemory(nodes []*v1.Node) resource.Quantity {
	totalMemory := resource.Quantity{}
	for _, node := range nodes {
		nodeAllocatableMemory := node.Status.Allocatable.Memory()
		totalMemory.Add(*nodeAllocatableMemory)
	}
	return totalMemory
}

func calculateTotalCpuLimit(pods []*v1.Pod) resource.Quantity {
	totalCpu := resource.Quantity{}
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			containerCpuLimit := container.Resources.Limits.Cpu()
			totalCpu.Add(*containerCpuLimit)
		}
		// Todo determine what to do about init contianers? How does Kubernetes scheduler handle these
	}
	return totalCpu
}

func calculateTotalMemoryLimit(pods []*v1.Pod) resource.Quantity {
	totalMemory := resource.Quantity{}
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			containerMemoryLimit := container.Resources.Limits.Memory()
			totalMemory.Add(*containerMemoryLimit)
		}
		// Todo determine what to do about init contianers? How does Kubernetes scheduler handle these
	}
	return totalMemory
}

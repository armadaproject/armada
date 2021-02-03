package scheduling

import (
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func CreateClusterSchedulingInfoReport(leaseRequest *api.LeaseRequest, nodeAllocations []*nodeTypeAllocation) *api.ClusterSchedulingInfoReport {
	return &api.ClusterSchedulingInfoReport{
		ClusterId:      leaseRequest.ClusterId,
		ReportTime:     time.Now(),
		NodeTypes:      extractNodeTypes(nodeAllocations),
		MinimumJobSize: leaseRequest.MinimumJobSize,
	}
}

func extractNodeTypes(allocations []*nodeTypeAllocation) []*api.NodeType {
	result := []*api.NodeType{}
	for _, n := range allocations {
		result = append(result, &api.NodeType{
			Taints:               n.taints,
			Labels:               n.labels,
			AllocatableResources: n.nodeSize,
		})
	}
	return result
}

func MatchSchedulingRequirements(job *api.Job, schedulingInfo *api.ClusterSchedulingInfoReport) bool {
	if !isLargeEnough(job, schedulingInfo.MinimumJobSize) {
		return false
	}
	for _, podSpec := range job.GetAllPodSpecs() {
		// TODO: make sure there are enough nodes available for all the job pods
		if !matchAnyNodeType(podSpec, schedulingInfo.NodeTypes) {
			return false
		}
	}
	return true
}

func isLargeEnough(job *api.Job, minimumJobSize common.ComputeResources) bool {
	resourceRequest := common.TotalJobResourceRequest(job)
	resourceRequest.Sub(minimumJobSize)
	return resourceRequest.IsValid()
}

func matchAnyNodeType(podSpec *v1.PodSpec, nodeTypes []*api.NodeType) bool {
	for _, nodeType := range nodeTypes {
		resourceRequest := common.TotalPodResourceRequest(podSpec).AsFloat()
		nodeResources := common.ComputeResources(nodeType.AllocatableResources).AsFloat()
		if fits(resourceRequest, nodeResources) && matchNodeSelector(podSpec, nodeType.Labels) && tolerates(podSpec, nodeType.Taints) {
			return true
		}
	}
	return false
}

func matchAnyNodeTypeAllocation(job *api.Job,
	nodeAllocations []*nodeTypeAllocation,
	alreadyConsumed nodeTypeUsedResources) (nodeTypeUsedResources, bool) {

	newlyConsumed := nodeTypeUsedResources{}

	for _, podSpec := range job.GetAllPodSpecs() {

		nodeType, ok := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)

		if !ok {
			return nodeTypeUsedResources{}, false
		}
		resourceRequest := common.TotalPodResourceRequest(podSpec).AsFloat()
		resourceRequest.Add(newlyConsumed[nodeType])
		newlyConsumed[nodeType] = resourceRequest
	}
	return newlyConsumed, true
}

func matchAnyNodeTypePodAllocation(
	podSpec *v1.PodSpec,
	nodeAllocations []*nodeTypeAllocation,
	alreadyConsumed nodeTypeUsedResources,
	newlyConsumed nodeTypeUsedResources) (*nodeTypeAllocation, bool) {
	resourceRequest := common.TotalPodResourceRequest(podSpec).AsFloat()

	for _, node := range nodeAllocations {
		available := node.availableResources.DeepCopy()
		available.Sub(alreadyConsumed[node])
		available.Sub(newlyConsumed[node])
		available.LimitWith(node.nodeSize.AsFloat())

		if fits(resourceRequest, available) && matchNodeSelector(podSpec, node.labels) && tolerates(podSpec, node.taints) {
			return node, true
		}
	}
	return nil, false
}

func fits(resourceRequest, availableResources common.ComputeResourcesFloat) bool {
	r := availableResources.DeepCopy()
	r.Sub(resourceRequest)
	return r.IsValid()
}

func matchNodeSelector(podSpec *v1.PodSpec, labels map[string]string) bool {
	for k, v := range podSpec.NodeSelector {
		if labels == nil || labels[k] != v {
			return false
		}
	}
	return true
}

func tolerates(podSpec *v1.PodSpec, taints []v1.Taint) bool {
	for _, taint := range taints {
		// check only hard constraints
		if taint.Effect == v1.TaintEffectPreferNoSchedule {
			continue
		}

		if !tolerationsTolerateTaint(podSpec.Tolerations, &taint) {
			return false
		}
	}
	return true
}

// https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/helpers.go#L427
func tolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(taint) {
			return true
		}
	}
	return false
}

func AggregateNodeTypeAllocations(nodes []api.NodeInfo) []*nodeTypeAllocation {
	nodeTypesIndex := map[string]*nodeTypeAllocation{}

	for _, n := range nodes {
		description := createNodeDescription(&n)
		typeDescription, exists := nodeTypesIndex[description]

		nodeAvailableResources := common.ComputeResources(n.AvailableResources).AsFloat()

		if !exists {
			typeDescription = &nodeTypeAllocation{
				taints:             n.Taints,
				labels:             n.Labels,
				nodeSize:           n.AllocatableResources,
				availableResources: nodeAvailableResources,
			}
			nodeTypesIndex[description] = typeDescription
		} else {
			typeDescription.availableResources.Add(nodeAvailableResources)
		}
	}

	result := []*nodeTypeAllocation{}
	for _, n := range nodeTypesIndex {
		result = append(result, n)
	}

	sort.Slice(result, func(i, j int) bool {
		// assign more tainted nodes first, then smaller nodes first
		return len(result[i].taints) > len(result[j].taints) ||
			len(result[i].taints) == len(result[j].taints) && result[j].nodeSize.Dominates(result[i].nodeSize)
	})

	return result
}

func createNodeDescription(n *api.NodeInfo) string {
	data := []string{}
	for k, v := range n.Labels {
		data = append(data, "l"+k+"="+v)
	}
	for _, t := range n.Taints {
		if t.Effect == v1.TaintEffectNoSchedule {
			data = append(data, "t"+t.Key+"="+t.Value)
		}
	}
	for k, v := range n.AllocatableResources {
		data = append(data, "t"+k+"="+v.String())
	}
	sort.Strings(data)
	return strings.Join(data, "|")
}

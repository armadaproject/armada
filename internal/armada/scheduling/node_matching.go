package scheduling

import (
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func CreateClusterSchedulingInfoReport(leaseRequest *api.LeaseRequest, nodeAllocations []*nodeTypeAllocation) *api.ClusterSchedulingInfoReport {
	return &api.ClusterSchedulingInfoReport{
		ClusterId:      leaseRequest.ClusterId,
		Pool:           leaseRequest.Pool,
		ReportTime:     time.Now(),
		NodeTypes:      extractNodeTypes(nodeAllocations),
		MinimumJobSize: leaseRequest.MinimumJobSize,
	}
}

func extractNodeTypes(allocations []*nodeTypeAllocation) []*api.NodeType {
	result := []*api.NodeType{}
	for _, n := range allocations {
		result = append(result, &n.nodeType)
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

func MatchSchedulingRequirementsOnAnyCluster(job *api.Job, allClusterSchedulingInfos map[string]*api.ClusterSchedulingInfoReport) bool {
	for _, schedulingInfo := range allClusterSchedulingInfos {
		if MatchSchedulingRequirements(job, schedulingInfo) {
			return true
		}
	}
	return false
}

func isLargeEnough(job *api.Job, minimumJobSize common.ComputeResources) bool {
	resourceRequest := common.TotalJobResourceRequest(job)
	resourceRequest.Sub(minimumJobSize)
	return resourceRequest.IsValid()
}

func matchAnyNodeType(podSpec *v1.PodSpec, nodeTypes []*api.NodeType) bool {

	podMatchingContext := NewPodMatchingContext(podSpec)

	for _, nodeType := range nodeTypes {
		nodeResources := common.ComputeResources(nodeType.AllocatableResources).AsFloat()

		if podMatchingContext.Matches(nodeType, nodeResources) {
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

	podMatchingContext := NewPodMatchingContext(podSpec)

	for _, node := range nodeAllocations {
		available := node.availableResources.DeepCopy()
		available.Sub(alreadyConsumed[node])
		available.Sub(newlyConsumed[node])
		available.LimitWith(common.ComputeResources(node.nodeType.AllocatableResources).AsFloat())

		if podMatchingContext.Matches(&node.nodeType, available) {
			return node, true
		}
	}
	return nil, false
}

func AggregateNodeTypeAllocations(nodes []api.NodeInfo) []*nodeTypeAllocation {
	nodeTypesIndex := map[string]*nodeTypeAllocation{}

	for _, n := range nodes {
		description := createNodeDescription(&n)
		typeDescription, exists := nodeTypesIndex[description]

		nodeAvailableResources := common.ComputeResources(n.AvailableResources).AsFloat()

		if !exists {
			typeDescription = &nodeTypeAllocation{
				nodeType: api.NodeType{
					Taints:               n.Taints,
					Labels:               n.Labels,
					AllocatableResources: n.AllocatableResources,
				},
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
		return len(result[i].nodeType.Taints) > len(result[j].nodeType.Taints) ||
			len(result[i].nodeType.Taints) == len(result[j].nodeType.Taints) && dominates(result[j].nodeType.AllocatableResources, result[i].nodeType.AllocatableResources)
	})

	return result
}

func dominates(a map[string]resource.Quantity, b map[string]resource.Quantity) bool {
	return (common.ComputeResources(a)).Dominates(common.ComputeResources(b))
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

package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func CreateClusterSchedulingInfoReport(leaseRequest *api.LeaseRequest, nodeAllocations []*nodeTypeAllocation, autoscalingPools []api.AutoscalingPool) *api.ClusterSchedulingInfoReport {
	return &api.ClusterSchedulingInfoReport{
		ClusterId:        leaseRequest.ClusterId,
		Pool:             leaseRequest.Pool,
		ReportTime:       time.Now(),
		NodeTypes:        extractNodeTypes(nodeAllocations),
		MinimumJobSize:   leaseRequest.MinimumJobSize,
		AutoscalingPools: autoscalingPools,
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

	jobAge := time.Now().Sub(job.Created)
	newlyConsumed := nodeTypeUsedResources{}

	for _, podSpec := range job.GetAllPodSpecs() {

		nodeType, ok := matchAnyNodeTypePodAllocation(podSpec, jobAge, nodeAllocations, alreadyConsumed, newlyConsumed)

		if !ok {
			return nodeTypeUsedResources{}, false
		}
		resourceRequest := common.TotalPodResourceRequest(podSpec).AsFloat()
		resourceRequest.Add(newlyConsumed[nodeType])
		newlyConsumed[nodeType] = resourceRequest
	}
	return newlyConsumed, true
}

func matchAnyNodeTypePodAllocation(podSpec *v1.PodSpec, jobAge time.Duration, nodeAllocations []*nodeTypeAllocation, alreadyConsumed nodeTypeUsedResources, newlyConsumed nodeTypeUsedResources) (*nodeTypeAllocation, bool) {
	resourceRequest := common.TotalPodResourceRequest(podSpec).AsFloat()

	for _, node := range nodeAllocations {
		available := node.availableResources.DeepCopy()
		available.Sub(alreadyConsumed[node])
		available.Sub(newlyConsumed[node])
		available.LimitWith(node.nodeSize.AsFloat())

		if jobAge > node.schedulingDelay && fits(resourceRequest, available) && matchNodeSelector(podSpec, node.labels) && tolerates(podSpec, node.taints) {
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
				nodeCount:          1,
			}
			nodeTypesIndex[description] = typeDescription
		} else {
			typeDescription.availableResources.Add(nodeAvailableResources)
			typeDescription.nodeCount += 1
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

func CalculateDormantResources(existingNodes []*nodeTypeAllocation, autoscalingPools []api.AutoscalingPool, schedulingDelay time.Duration) []*nodeTypeAllocation {
	if len(autoscalingPools) == 0 {
		return []*nodeTypeAllocation{}
	}

	poolNodeTypesIndex := map[string]*nodeTypeAllocation{}
	poolNodeTypes := []*nodeTypeAllocation{}

	for _, pool := range autoscalingPools {
		nodeSize := common.ComputeResources(pool.NodeType.AllocatableResources)
		desc := createNodeDescription(&api.NodeInfo{
			Taints:               pool.NodeType.Taints,
			Labels:               pool.NodeType.Labels,
			AllocatableResources: nodeSize,
		})

		n := &nodeTypeAllocation{
			taints:             pool.NodeType.Taints,
			labels:             pool.NodeType.Labels,
			nodeSize:           nodeSize,
			availableResources: nodeSize.Mul(float64(pool.MaxNodeCount)),
			schedulingDelay:    schedulingDelay,
		}
		poolNodeTypes = append(poolNodeTypes, n)
		poolNodeTypesIndex[desc] = n
	}

	for _, existing := range existingNodes {
		desc := createNodeDescription(&api.NodeInfo{
			Taints:               existing.taints,
			Labels:               existing.labels,
			AllocatableResources: existing.nodeSize,
		})
		pool, ok := poolNodeTypesIndex[desc]
		if !ok {
			// TODO: can we improvise and find closest node here?
			log.Errorf("Node %s does not belong to any node type.", desc)
		} else {
			pool.availableResources.Sub(existing.nodeSize.AsFloat().Mul(float64(existing.nodeCount)))
		}
	}
	return poolNodeTypes
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
		data = append(data, "t"+k+"="+roundQuantityString(v, 3))
	}
	sort.Strings(data)
	return strings.Join(data, "|")
}

// Produce canonical string representation of rounded quantity,
// for example: 123456 => 123e3, 0.01 => 100e-4, 128Gi => 137e9
func roundQuantityString(v resource.Quantity, precision int) string {
	d := v.AsDec()
	scale := int(d.Scale())
	value := d.UnscaledBig().String()

	scale -= len(value) - precision
	if len(value) > precision {
		value = value[0:precision]
	} else {
		value = value + strings.Repeat("0", precision-len(value))
	}
	return fmt.Sprintf("%se%d", value, -scale)
}

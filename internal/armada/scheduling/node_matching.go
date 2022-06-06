package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/armadaerrors"
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

// MatchSchedulingRequirementsOnAnyCluster returns true if the provided job can be scheduled.
// If returning false, the reason for not being able to schedule the pod is indicated by the returned error,
// which is of type *armadaerrors.ErrPodUnschedulable.
func MatchSchedulingRequirementsOnAnyCluster(job *api.Job, allClusterSchedulingInfos map[string]*api.ClusterSchedulingInfoReport) (bool, error) {
	var errs []error
	for _, schedulingInfo := range allClusterSchedulingInfos {
		if ok, err := MatchSchedulingRequirements(job, schedulingInfo); ok {
			return true, nil
		} else {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		errs = append(errs, fmt.Errorf("no matching node types available"))
	}
	// Return a merged error report.
	// The idea is that users don't care how nodes are split between clusters.
	// These errors do not include info about which pod in the job caused the error.
	// However, this is fine since we currently only support single-pod jobs.
	return false, armadaerrors.NewCombinedErrPodUnschedulable(errs...)
}

func MatchSchedulingRequirements(job *api.Job, schedulingInfo *api.ClusterSchedulingInfoReport) (bool, error) {
	if !isLargeEnough(job, schedulingInfo.MinimumJobSize) {
		err := &armadaerrors.ErrPodUnschedulable{}
		err = err.Add(fmt.Sprintf("pod resource requests too low; the minimum allowed is %v", schedulingInfo.MinimumJobSize), len(schedulingInfo.NodeTypes))
		return false, err
	}
	for i, podSpec := range job.GetAllPodSpecs() {
		// TODO: make sure there are enough nodes available for all the job pods.
		if ok, err := matchAnyNodeType(podSpec, schedulingInfo.NodeTypes); !ok {
			if err != nil {
				return false, err
			} else {
				return false, errors.Errorf("%d-th pod can't be scheduled", i)
			}
		}
	}
	return true, nil
}

func isLargeEnough(job *api.Job, minimumJobSize common.ComputeResources) bool {
	resourceRequest := common.TotalJobResourceRequest(job)
	resourceRequest.Sub(minimumJobSize)
	return resourceRequest.IsValid()
}

// matchAnyNodeType returns true if the pod can be scheduled on at least one node type.
// If not, an error is returned indicating why the pod can't be scheduled.
// The error is of type *armadaerrors.ErrPodUnschedulable.
func matchAnyNodeType(podSpec *v1.PodSpec, nodeTypes []*api.NodeType) (bool, error) {
	if len(nodeTypes) == 0 {
		return false, errors.Errorf("no node types available")
	}

	var result *armadaerrors.ErrPodUnschedulable
	podMatchingContext := NewPodMatchingContext(podSpec)
	for _, nodeType := range nodeTypes {
		nodeResources := common.ComputeResources(nodeType.AllocatableResources).AsFloat()
		if ok, err := podMatchingContext.Matches(nodeType, nodeResources); ok {
			return true, nil
		} else if err != nil {
			result = result.Add(err.Error(), 1)
		} else {
			result = result.Add("unknown reason", 1)
		}
	}
	return false, result
}

func matchAnyNodeTypeAllocation(job *api.Job,
	nodeAllocations []*nodeTypeAllocation,
	alreadyConsumed nodeTypeUsedResources) (nodeTypeUsedResources, bool, error) {

	newlyConsumed := nodeTypeUsedResources{}

	for _, podSpec := range job.GetAllPodSpecs() {

		nodeType, ok, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)

		if !ok {
			return nodeTypeUsedResources{}, false, err
		}
		resourceRequest := common.TotalPodResourceRequest(podSpec).AsFloat()
		resourceRequest.Add(newlyConsumed[nodeType])
		newlyConsumed[nodeType] = resourceRequest
	}
	return newlyConsumed, true, nil
}

func matchAnyNodeTypePodAllocation(
	podSpec *v1.PodSpec,
	nodeAllocations []*nodeTypeAllocation,
	alreadyConsumed nodeTypeUsedResources,
	newlyConsumed nodeTypeUsedResources) (*nodeTypeAllocation, bool, error) {

	if len(nodeAllocations) == 0 {
		return nil, false, fmt.Errorf("no nodes available")
	}

	podMatchingContext := NewPodMatchingContext(podSpec)
	var result *armadaerrors.ErrPodUnschedulable
	for _, node := range nodeAllocations {
		available := node.availableResources.DeepCopy()
		available.Sub(alreadyConsumed[node])
		available.Sub(newlyConsumed[node])
		available.LimitWith(common.ComputeResources(node.nodeType.AllocatableResources).AsFloat())

		if ok, err := podMatchingContext.Matches(&node.nodeType, available); ok {
			return node, true, nil
		} else if err != nil {
			result = result.Add(err.Error(), 1)
		} else {
			result = result.Add("unknown reason", 1)
		}
	}
	return nil, false, result
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

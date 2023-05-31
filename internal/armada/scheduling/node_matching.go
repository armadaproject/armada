package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
)

func CreateClusterSchedulingInfoReport(leaseRequest *api.StreamingLeaseRequest, nodeAllocations []*nodeTypeAllocation) *api.ClusterSchedulingInfoReport {
	return &api.ClusterSchedulingInfoReport{
		ClusterId:      leaseRequest.ClusterId,
		Pool:           leaseRequest.Pool,
		ReportTime:     time.Now(),
		NodeTypes:      extractNodeTypes(nodeAllocations),
		MinimumJobSize: leaseRequest.MinimumJobSize,
	}
}

func extractNodeTypes(allocations []*nodeTypeAllocation) []*api.NodeType {
	var result []*api.NodeType
	for _, n := range allocations {
		result = append(result, &n.nodeType)
	}
	return result
}

// MatchSchedulingRequirementsOnAnyCluster returns true if the provided job can be scheduled.
// If returning false, the reason for not being able to schedule the pod is indicated by the returned error,
// which is of type *armadaerrors.ErrPodUnschedulable.
func MatchSchedulingRequirementsOnAnyCluster(
	job *api.Job,
	allClusterSchedulingInfos map[string]*api.ClusterSchedulingInfoReport,
) (bool, error) {
	var errs []error
	for _, schedulingInfo := range allClusterSchedulingInfos {
		if ok, err := MatchSchedulingRequirements(job, schedulingInfo); ok {
			return true, nil
		} else {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		errs = append(errs, errors.Errorf("no matching node types available"))
	}
	// Return a merged error report.
	// The idea is that users don't care how nodes are split between clusters.
	// These errors do not include info about which pod in the job caused the error.
	// However, this is fine since we currently only support single-pod jobs.
	return false, armadaerrors.NewCombinedErrPodUnschedulable(errs...)
}

func MatchSchedulingRequirements(
	job *api.Job,
	schedulingInfo *api.ClusterSchedulingInfoReport,
) (bool, error) {
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
			}
			return false, errors.Errorf("%d-th pod can't be scheduled", i)
		}
	}
	return true, nil
}

func isLargeEnough(job *api.Job, minimumJobSize armadaresource.ComputeResources) bool {
	if len(minimumJobSize) == 0 {
		return true
	}
	rl := job.TotalResourceRequestAsResourceList()
	for t, limit := range minimumJobSize {
		q := rl.Get(t)
		if limit.Cmp(q) == 1 {
			return false
		}
	}
	return true
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
		nodeResources := armadaresource.ComputeResources(nodeType.AllocatableResources).AsFloat().DeepCopy()
		ok, err := podMatchingContext.Matches(nodeType, nodeResources)
		switch {
		case ok:
			return true, nil
		case err != nil:
			result = result.Add(err.Error(), 1)
		default:
			result = result.Add("unknown reason", 1)
		}
	}
	return false, result
}

func matchAnyNodeTypePodAllocation(
	podSpec *v1.PodSpec,
	nodeAllocations []*nodeTypeAllocation,
	alreadyConsumed nodeTypeUsedResources,
	newlyConsumed nodeTypeUsedResources,
) (*nodeTypeAllocation, bool, error) {
	if len(nodeAllocations) == 0 {
		return nil, false, errors.Errorf("no nodes available")
	}

	podMatchingContext := NewPodMatchingContext(podSpec)
	var result *armadaerrors.ErrPodUnschedulable
	for _, node := range nodeAllocations {
		available := node.availableResources.DeepCopy()
		available.Sub(alreadyConsumed[node])
		available.Sub(newlyConsumed[node])
		available.LimitWith(armadaresource.ComputeResources(node.nodeType.AllocatableResources).AsFloat())

		resources := available
		ok, err := podMatchingContext.Matches(&node.nodeType, resources)
		switch {
		case ok:
			return node, true, nil
		case err != nil:
			result = result.Add(err.Error(), 1)
		default:
			result = result.Add("unknown reason", 1)
		}
	}
	return nil, false, result
}

// AggregateNodeTypeAllocations computes the total available resources for each node type.
func AggregateNodeTypeAllocations(nodes []api.NodeInfo) []*nodeTypeAllocation {
	nodeTypesIndex := map[string]*nodeTypeAllocation{}

	for i, n := range nodes {
		description := createNodeDescription(&nodes[i])
		typeDescription, exists := nodeTypesIndex[description]

		nodeAvailableResources := armadaresource.ComputeResources(n.AvailableResources).AsFloat()
		nodeTotalResources := armadaresource.ComputeResources(n.TotalResources).AsFloat()
		nodeAllocatedResources := make(map[int32]armadaresource.ComputeResourcesFloat)
		for k, v := range n.AllocatedResources {
			nodeAllocatedResources[k] = armadaresource.ComputeResources(v.Resources).AsFloat()
		}

		if !exists {
			typeDescription = &nodeTypeAllocation{
				nodeType: api.NodeType{
					Taints:               n.Taints,
					Labels:               n.Labels,
					AllocatableResources: n.AllocatableResources,
				},
				availableResources: nodeAvailableResources,
				totalResources:     nodeTotalResources,
				allocatedResources: nodeAllocatedResources,
			}
		} else {
			typeDescription.totalResources.Add(nodeTotalResources)
			typeDescription.availableResources.Add(nodeAvailableResources)

			for priority, resources := range nodeAllocatedResources {
				totalAllocatedResources, exists := typeDescription.allocatedResources[priority]
				if exists {
					totalAllocatedResources.Add(resources)
				} else {
					typeDescription.allocatedResources[priority] = resources
				}
			}
		}
		nodeTypesIndex[description] = typeDescription
	}

	var result []*nodeTypeAllocation
	for _, n := range nodeTypesIndex {
		result = append(result, n)
	}

	sort.Slice(result, func(i, j int) bool {
		// assign more tainted nodes first, then smaller nodes first
		return len(result[i].nodeType.Taints) > len(result[j].nodeType.Taints) ||
			len(result[i].nodeType.Taints) == len(result[j].nodeType.Taints) &&
				dominates(result[j].nodeType.AllocatableResources, result[i].nodeType.AllocatableResources)
	})

	return result
}

func dominates(a map[string]resource.Quantity, b map[string]resource.Quantity) bool {
	return (armadaresource.ComputeResources(a)).Dominates(armadaresource.ComputeResources(b))
}

// createNodeDescription maps the labels, taints, and allocatable resources of a node to a unique string.
func createNodeDescription(n *api.NodeInfo) string {
	data := make([]string, 0, len(n.Labels)+len(n.Taints)+len(n.AllocatableResources))
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

package schedulerobjects

import (
	"math/rand"

	"github.com/segmentio/fasthash/fnv1a"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

type SchedulingKey uint64

func (req *PodRequirements) GetAffinityNodeSelector() *v1.NodeSelector {
	affinity := req.Affinity
	if affinity == nil {
		return nil
	}
	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil
	}
	return nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

// SchedulingKeyGenerator is used to generate scheduling keys efficiently without requiring allocs.
// A scheduling key is the canonical hash of the scheduling requirements of a job.
//
// Fields are separated by =, $, &, and =, since these characters are not allowed in taints and labels; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
type SchedulingKeyGenerator struct {
	// Prefix is written into the hash before any other data.
	// Using a randomly chosen prefix in each scheduling round makes hash collisions persisting across rounds less likely.
	prefix                        uint64
	stringBuffer                  []string
	byteBuffer                    []byte
	nodeSelectorTermBuffer        []v1.NodeSelectorTerm
	nodeSelectorRequirementBuffer []v1.NodeSelectorRequirement
	tolerationBuffer              []v1.Toleration
	resourceNameBuffer            []v1.ResourceName
}

func NewSchedulingKeyGenerator() *SchedulingKeyGenerator {
	// Create buffers with some initial capacity to reduce dynamic allocs.
	return &SchedulingKeyGenerator{
		prefix:                        uint64(rand.Int63()),
		stringBuffer:                  make([]string, 0, 16),
		byteBuffer:                    make([]byte, 0, 1024),
		nodeSelectorTermBuffer:        make([]v1.NodeSelectorTerm, 16),
		nodeSelectorRequirementBuffer: make([]v1.NodeSelectorRequirement, 16),
		tolerationBuffer:              make([]v1.Toleration, 0, 16),
		resourceNameBuffer:            make([]v1.ResourceName, 0, 4),
	}
}

func (skg *SchedulingKeyGenerator) Key(
	nodeSelector map[string]string,
	affinity *v1.Affinity,
	tolerations []v1.Toleration,
	requests v1.ResourceList,
	priority int32,
) SchedulingKey {
	h := fnv1a.Init64
	h = fnv1a.AddUint64(h, skg.prefix)
	h = skg.addNodeSelector64(h, nodeSelector)
	h = skg.addAffinity64(h, affinity)
	h = skg.addTolerations64(h, tolerations)
	h = skg.addResourceList64(h, requests)
	h = fnv1a.AddUint64(h, uint64(priority))
	return SchedulingKey(h)
}

func (skg *SchedulingKeyGenerator) addNodeSelector64(h uint64, nodeSelector map[string]string) uint64 {
	skg.stringBuffer = skg.stringBuffer[0:0]
	for _, key := range nodeSelector {
		skg.stringBuffer = append(skg.stringBuffer, key)
	}
	slices.Sort(skg.stringBuffer)
	for _, key := range skg.stringBuffer {
		value := nodeSelector[key]
		h = fnv1a.AddString64(h, key)
		h = fnv1a.AddString64(h, "=")
		h = fnv1a.AddString64(h, value)
		h = fnv1a.AddString64(h, "$")
	}
	h = fnv1a.AddString64(h, "&")
	return h
}

// addAffinity64 writes a v1.Affinity into the hash.
// Only NodeAffinity (i.e., not PodAffinity) and RequiredDuringSchedulingIgnoredDuringExecution fields are considered.
func (skg *SchedulingKeyGenerator) addAffinity64(h uint64, affinity *v1.Affinity) uint64 {
	if affinity == nil {
		return h
	}
	if affinity.NodeAffinity == nil {
		return h
	}
	h = skg.addAffinityNodeSelector64(h, affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
	return h
}

func (skg *SchedulingKeyGenerator) addAffinityNodeSelector64(h uint64, nodeSelector *v1.NodeSelector) uint64 {
	if nodeSelector == nil {
		return h
	}
	skg.nodeSelectorTermBuffer = skg.nodeSelectorTermBuffer[0:0]
	skg.nodeSelectorTermBuffer = append(skg.nodeSelectorTermBuffer, nodeSelector.NodeSelectorTerms...)
	slices.SortFunc(skg.nodeSelectorTermBuffer, lessNodeSelectorTerm)
	for _, nodeSelectorTerm := range skg.nodeSelectorTermBuffer {
		h = skg.addNodeSelectorRequirements64(h, nodeSelectorTerm.MatchExpressions)
		h = skg.addNodeSelectorRequirements64(h, nodeSelectorTerm.MatchFields)
	}
	if len(skg.nodeSelectorTermBuffer) > 0 {
		h = fnv1a.AddString64(h, "&")
	}
	return h
}

func (skg *SchedulingKeyGenerator) addNodeSelectorRequirements64(h uint64, nodeSelectorRequirements []v1.NodeSelectorRequirement) uint64 {
	skg.nodeSelectorRequirementBuffer = skg.nodeSelectorRequirementBuffer[0:0]
	skg.nodeSelectorRequirementBuffer = append(skg.nodeSelectorRequirementBuffer, nodeSelectorRequirements...)
	slices.SortFunc(skg.nodeSelectorRequirementBuffer, lessNodeSelectorRequirement)
	for _, nodeSelectorRequirement := range skg.nodeSelectorRequirementBuffer {
		h = fnv1a.AddString64(h, nodeSelectorRequirement.Key)
		h = fnv1a.AddString64(h, "=")
		for _, value := range nodeSelectorRequirement.Values {
			h = fnv1a.AddString64(h, value)
			h = fnv1a.AddString64(h, "$")
		}
		h = fnv1a.AddString64(h, ":")
		h = fnv1a.AddString64(h, string(nodeSelectorRequirement.Operator))
		h = fnv1a.AddString64(h, "$")
	}
	if len(skg.nodeSelectorRequirementBuffer) > 0 {
		h = fnv1a.AddString64(h, "&")
	}
	return h
}

func (skg *SchedulingKeyGenerator) addTolerations64(h uint64, tolerations []v1.Toleration) uint64 {
	skg.tolerationBuffer = skg.tolerationBuffer[0:0]
	skg.tolerationBuffer = append(skg.tolerationBuffer, tolerations...)
	slices.SortFunc(skg.tolerationBuffer, lessToleration)
	for _, toleration := range skg.tolerationBuffer {
		h = fnv1a.AddString64(h, toleration.Key)
		h = fnv1a.AddString64(h, "=")
		h = fnv1a.AddString64(h, toleration.Value)
		h = fnv1a.AddString64(h, ":")
		h = fnv1a.AddString64(h, string(toleration.Operator))
		h = fnv1a.AddString64(h, ":")
		h = fnv1a.AddString64(h, string(toleration.Effect))
		h = fnv1a.AddString64(h, "$")
	}
	if len(tolerations) > 0 {
		h = fnv1a.AddString64(h, "&")
	}
	return h
}

func (skg *SchedulingKeyGenerator) addResourceList64(h uint64, resourceList v1.ResourceList) uint64 {
	skg.resourceNameBuffer = skg.resourceNameBuffer[0:0]
	for t, q := range resourceList {
		if !q.IsZero() {
			// Zero-valued requests don't affect scheduling.
			skg.resourceNameBuffer = append(skg.resourceNameBuffer, t)
		}
	}
	slices.Sort(skg.resourceNameBuffer)
	for _, t := range skg.resourceNameBuffer {
		q := resourceList[t]
		h = fnv1a.AddString64(h, string(t))
		h = fnv1a.AddString64(h, "=")
		h = fnv1a.AddUint64(h, uint64(q.MilliValue()))
		h = fnv1a.AddString64(h, "$")
	}
	if len(skg.resourceNameBuffer) > 0 {
		h = fnv1a.AddString64(h, "&")
	}
	return h
}

// ClearCachedSchedulingKey clears any cached scheduling keys.
// Necessary after changing scheduling requirements to avoid inconsistency.
func (jobSchedulingInfo *JobSchedulingInfo) ClearCachedSchedulingKey() {
	if jobSchedulingInfo == nil {
		return
	}
	for _, objReq := range jobSchedulingInfo.ObjectRequirements {
		if req := objReq.GetPodRequirements(); req != nil {
			req.ClearCachedSchedulingKey()
		}
	}
}

// ClearCachedSchedulingKey clears any cached scheduling key.
// Necessary after changing scheduling requirements to avoid inconsistency.
func (req *PodRequirements) ClearCachedSchedulingKey() {
	req.CachedSchedulingKey = nil
}

func lessToleration(a, b v1.Toleration) bool {
	if a.Key < b.Key {
		return true
	} else if a.Key > b.Key {
		return false
	}
	if a.Value < b.Value {
		return true
	} else if a.Value > b.Value {
		return false
	}
	if string(a.Operator) < string(b.Operator) {
		return true
	} else if string(a.Operator) > string(b.Operator) {
		return false
	}
	if string(a.Effect) < string(b.Effect) {
		return true
	} else if string(a.Effect) > string(b.Effect) {
		return false
	}
	return true
}

func lessNodeSelectorTerm(a, b v1.NodeSelectorTerm) bool {
	// For simplicity, we consider only the length and not the contents of MatchExpressions/MatchFields.
	// Hence, the hash may depend on their order.
	if len(a.MatchExpressions) < len(b.MatchExpressions) {
		return true
	} else if len(a.MatchExpressions) > len(b.MatchExpressions) {
		return false
	}
	if len(a.MatchFields) < len(b.MatchFields) {
		return true
	} else if len(a.MatchFields) > len(b.MatchFields) {
		return false
	}
	return true
}

func lessNodeSelectorRequirement(a, b v1.NodeSelectorRequirement) bool {
	if a.Key < b.Key {
		return true
	} else if a.Key > b.Key {
		return false
	}
	if string(a.Operator) < string(b.Operator) {
		return true
	} else if string(a.Operator) > string(b.Operator) {
		return false
	}
	if len(a.Values) < len(b.Values) {
		return true
	} else if len(a.Values) > len(b.Values) {
		return false
	}
	return true
}

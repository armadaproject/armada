package schedulerobjects

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/minio/highwayhash"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

type SchedulingKey [highwayhash.Size]byte

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

// SchedulingKeyGenerator is used to generate scheduling keys efficiently.
// A scheduling key is the canonical hash of the scheduling requirements of a job.
// All memory is allocated up-front and re-used. Not thread-safe.
type SchedulingKeyGenerator struct {
	s      PodRequirementsSerialiser
	key    []byte
	buffer []byte
}

func NewSchedulingKeyGenerator() *SchedulingKeyGenerator {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// This should never happen.
		panic(err)
	}
	return &SchedulingKeyGenerator{
		s:      *NewPodRequirementsSerialiser(),
		key:    key,
		buffer: make([]byte, 2048),
	}
}

func (skg *SchedulingKeyGenerator) Key(
	nodeSelector map[string]string,
	affinity *v1.Affinity,
	tolerations []v1.Toleration,
	requests v1.ResourceList,
	priority int32,
) SchedulingKey {
	skg.buffer = skg.buffer[0:0]
	skg.buffer = skg.s.AppendRequirements(
		skg.buffer,
		nodeSelector,
		affinity,
		tolerations,
		requests,
		priority,
	)
	return highwayhash.Sum(skg.buffer, skg.key)
}

// PodRequirementsSerialiser produces the canonical byte representation of a set of pod scheduling requirements.
// The resulting byte array can, e.g., be used to produce a hash guaranteed to be equal for equivalent requirements.
// Not thread-safe.
//
// Fields are separated by =, $, &, and =, since these characters are not allowed in taints and labels; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
type PodRequirementsSerialiser struct {
	stringBuffer                  []string
	byteBuffer                    []byte
	nodeSelectorRequirementBuffer []v1.NodeSelectorRequirement
	tolerationBuffer              []v1.Toleration
	resourceNameBuffer            []v1.ResourceName
}

func NewPodRequirementsSerialiser() *PodRequirementsSerialiser {
	return &PodRequirementsSerialiser{
		stringBuffer:                  make([]string, 0, 16),
		byteBuffer:                    make([]byte, 0, 1024),
		nodeSelectorRequirementBuffer: make([]v1.NodeSelectorRequirement, 16),
		tolerationBuffer:              make([]v1.Toleration, 0, 16),
		resourceNameBuffer:            make([]v1.ResourceName, 0, 4),
	}
}

func (skg *PodRequirementsSerialiser) AppendRequirements(
	out []byte,
	nodeSelector map[string]string,
	affinity *v1.Affinity,
	tolerations []v1.Toleration,
	requests v1.ResourceList,
	priority int32,
) []byte {
	out = skg.AppendNodeSelector(out, nodeSelector)
	out = skg.AppendAffinity(out, affinity)
	out = skg.AppendTolerations(out, tolerations)
	out = skg.AppendResourceList(out, requests)
	out = binary.LittleEndian.AppendUint32(out, uint32(priority))
	return out
}

func (skg *PodRequirementsSerialiser) AppendNodeSelector(out []byte, nodeSelector map[string]string) []byte {
	skg.stringBuffer = skg.stringBuffer[0:0]
	for _, key := range nodeSelector {
		skg.stringBuffer = append(skg.stringBuffer, key)
	}
	slices.Sort(skg.stringBuffer)
	for _, key := range skg.stringBuffer {
		value := nodeSelector[key]
		out = append(out, []byte(key)...)
		out = append(out, []byte("=")...)
		out = append(out, []byte(value)...)
		out = append(out, []byte("$")...)
	}
	out = append(out, []byte("&")...)
	return out
}

// AppendAffinity writes a v1.Affinity into the hash.
// Only NodeAffinity (i.e., not PodAffinity) and RequiredDuringSchedulingIgnoredDuringExecution fields are considered.
func (skg *PodRequirementsSerialiser) AppendAffinity(out []byte, affinity *v1.Affinity) []byte {
	if affinity == nil {
		return out
	}
	if affinity.NodeAffinity == nil {
		return out
	}
	return skg.AppendAffinityNodeSelector(out, affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
}

func (skg *PodRequirementsSerialiser) AppendAffinityNodeSelector(out []byte, nodeSelector *v1.NodeSelector) []byte {
	if nodeSelector == nil {
		return out
	}
	// For simplicity, these are not sorted. Hence, the hash may depend on their order.
	for _, nodeSelectorTerm := range nodeSelector.NodeSelectorTerms {
		out = skg.AppendNodeSelectorRequirements(out, nodeSelectorTerm.MatchExpressions)
		out = skg.AppendNodeSelectorRequirements(out, nodeSelectorTerm.MatchFields)
	}
	if len(nodeSelector.NodeSelectorTerms) > 0 {
		out = append(out, []byte("&")...)
	}
	return out
}

func (skg *PodRequirementsSerialiser) AppendNodeSelectorRequirements(out []byte, nodeSelectorRequirements []v1.NodeSelectorRequirement) []byte {
	skg.nodeSelectorRequirementBuffer = skg.nodeSelectorRequirementBuffer[0:0]
	skg.nodeSelectorRequirementBuffer = append(skg.nodeSelectorRequirementBuffer, nodeSelectorRequirements...)
	slices.SortFunc(skg.nodeSelectorRequirementBuffer, lessNodeSelectorRequirement)
	for _, nodeSelectorRequirement := range skg.nodeSelectorRequirementBuffer {
		out = append(out, []byte(nodeSelectorRequirement.Key)...)
		out = append(out, []byte("=")...)
		for _, value := range nodeSelectorRequirement.Values {
			out = append(out, []byte(value)...)
			out = append(out, []byte("$")...)
		}
		out = append(out, []byte(":")...)
		out = append(out, []byte(nodeSelectorRequirement.Operator)...)
		out = append(out, []byte("$")...)
	}
	if len(skg.nodeSelectorRequirementBuffer) > 0 {
		out = append(out, []byte("&")...)
	}
	return out
}

func (skg *PodRequirementsSerialiser) AppendTolerations(out []byte, tolerations []v1.Toleration) []byte {
	skg.tolerationBuffer = skg.tolerationBuffer[0:0]
	skg.tolerationBuffer = append(skg.tolerationBuffer, tolerations...)
	slices.SortFunc(skg.tolerationBuffer, lessToleration)
	for _, toleration := range skg.tolerationBuffer {
		out = append(out, []byte(toleration.Key)...)
		out = append(out, []byte("=")...)
		out = append(out, []byte(toleration.Value)...)
		out = append(out, []byte(":")...)
		out = append(out, []byte(toleration.Operator)...)
		out = append(out, []byte(":")...)
		out = append(out, []byte((toleration.Effect))...)
		out = append(out, []byte("$")...)
	}
	if len(tolerations) > 0 {
		out = append(out, []byte("&")...)
	}
	return out
}

func (skg *PodRequirementsSerialiser) AppendResourceList(out []byte, resourceList v1.ResourceList) []byte {
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
		out = append(out, []byte(t)...)
		out = append(out, []byte("=")...)
		out = binary.LittleEndian.AppendUint64(out, uint64(q.MilliValue()))
		out = append(out, []byte("$")...)
	}
	if len(skg.resourceNameBuffer) > 0 {
		out = append(out, "&"...)
	}
	return out
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

package schedulerobjects

import (
	"io"
	"strconv"
	"strings"

	"crypto/sha1"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
)

const SchedulingKeySize = 20

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

// SchedulingKey returns the canonical hash of the scheduling requirements of a job.
//
// The hash is of size SchedulingKeySize and is guaranteed to always be the same for equivalent requirements,
// unless Affinity is used, in which case they may differ as a result of unordered map keys.
func (jobSchedulingInfo *JobSchedulingInfo) SchedulingKey() ([SchedulingKeySize]byte, bool) {
	if jobSchedulingInfo == nil {
		return [SchedulingKeySize]byte{}, false
	}
	for _, objReq := range jobSchedulingInfo.ObjectRequirements {
		if req := objReq.GetPodRequirements(); req != nil {
			return req.SchedulingKey(), true
		}
	}
	return [SchedulingKeySize]byte{}, false
}

// SchedulingKey returns the canonical hash of the scheduling requirements of a pod.
//
// The hash is of size SchedulingKeySize and is guaranteed to always be the same for equivalent requirements,
// unless Affinity is used, in which case they may differ as a result of unordered map keys.
func (req *PodRequirements) SchedulingKey() [SchedulingKeySize]byte {
	if req.CachedSchedulingKey == nil {
		// Cache the key such that the next invocation returns a pre-computed key.
		schedulingKey := schedulingKeyFromPodRequirements(req)
		req.CachedSchedulingKey = schedulingKey[:]
	}
	return [SchedulingKeySize]byte(req.CachedSchedulingKey)
}

func schedulingKeyFromPodRequirements(req *PodRequirements) [SchedulingKeySize]byte {
	// We separate taints/labels by $, labels and values by =, and and groups by &,
	// since these characters are not allowed in taints and labels; see
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
	// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
	h := sha1.New()

	nodeSelectorKeys := maps.Keys(req.NodeSelector)
	slices.Sort(nodeSelectorKeys)
	for _, key := range nodeSelectorKeys {
		value := req.NodeSelector[key]
		io.WriteString(h, key)
		io.WriteString(h, "=")
		io.WriteString(h, value)
		io.WriteString(h, "$")
	}
	io.WriteString(h, "&")

	if req.Affinity != nil {
		io.WriteString(h, req.Affinity.String())
	}
	io.WriteString(h, "&")

	tolerations := slices.Clone(req.Tolerations)
	slices.SortFunc(tolerations, lessToleration)
	for _, toleration := range tolerations {
		io.WriteString(h, toleration.Key)
		io.WriteString(h, "=")
		io.WriteString(h, toleration.Value)
		io.WriteString(h, ":")
		io.WriteString(h, string(toleration.Operator))
		io.WriteString(h, ":")
		io.WriteString(h, string(toleration.Effect))
		io.WriteString(h, "$")
	}
	io.WriteString(h, "&")

	io.WriteString(h, strconv.Itoa(int(req.Priority)))
	io.WriteString(h, "&")

	requestKeys := maps.Keys(req.ResourceRequirements.Requests)
	requestKeys = armadaslices.Filter(
		requestKeys,
		func(key v1.ResourceName) bool {
			q := req.ResourceRequirements.Requests[key]
			return q.Cmp(resource.Quantity{}) != 0
		},
	)
	slices.Sort(requestKeys)
	for _, key := range requestKeys {
		value := req.ResourceRequirements.Requests[key]
		io.WriteString(h, string(key))
		io.WriteString(h, "=")
		h.Write(EncodeQuantity(value))
		io.WriteString(h, "$")
	}

	return [SchedulingKeySize]byte(h.Sum(nil))
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
	if cmp := strings.Compare(a.Key, b.Key); cmp == -1 {
		return true
	} else if cmp == 1 {
		return false
	}
	if cmp := strings.Compare(a.Value, b.Value); cmp == -1 {
		return true
	} else if cmp == 1 {
		return false
	}
	if cmp := strings.Compare(string(a.Operator), string(b.Operator)); cmp == -1 {
		return true
	} else if cmp == 1 {
		return false
	}
	if cmp := strings.Compare(string(a.Effect), string(b.Effect)); cmp == -1 {
		return true
	} else if cmp == 1 {
		return false
	}
	return true
}

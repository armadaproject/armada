package config

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/client/util"
)

// NodeProfile describes the shape of a single simulated node: what it's allocatable for and
// what labels/taints it carries. It represents exactly one node, not a count of them - to
// stand up several, reference the same profile file from a NodeGroup. Shared by both the KWOK
// target (mapped into a real v1.Node) and the fake-executor target (mapped into
// executor/fake/context.NodeSpec) so a hardware shape only needs to be described once.
type NodeProfile struct {
	Name        string                                `json:"name"`
	Allocatable map[v1.ResourceName]resource.Quantity `json:"allocatable"`
	Labels      map[string]string                     `json:"labels,omitempty"`
	Taints      []v1.Taint                            `json:"taints,omitempty"`
}

// NodeGroup is the set of nodes a KwokTarget/FakeExecutorTarget stands up: an arbitrary number
// of (profile, count) members, combined together. This lets one group mix shapes, e.g. a rack
// of GB200 slices alongside a handful of CPU-only nodes.
type NodeGroup []NodeGroupMember

// NodeGroupMember points at a NodeProfile file and says how many nodes of that shape to create.
type NodeGroupMember struct {
	NodeProfile string `json:"nodeProfile"`
	Count       int    `json:"count"`
}

// LoadNodeProfile reads a NodeProfile from a YAML file at path.
func LoadNodeProfile(path string) (*NodeProfile, error) {
	profile := &NodeProfile{}
	if err := util.BindJsonOrYaml(path, profile); err != nil {
		return nil, err
	}
	return profile, nil
}

// ResolvedNodeGroupMember is a NodeGroupMember with its NodeProfile file already loaded.
type ResolvedNodeGroupMember struct {
	Profile *NodeProfile
	Count   int
}

// LoadNodeGroup loads every member profile referenced by group, in order.
func LoadNodeGroup(group NodeGroup) ([]ResolvedNodeGroupMember, error) {
	resolved := make([]ResolvedNodeGroupMember, 0, len(group))
	for _, member := range group {
		profile, err := LoadNodeProfile(member.NodeProfile)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, ResolvedNodeGroupMember{Profile: profile, Count: member.Count})
	}
	return resolved, nil
}

package nodedb

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

func TestFromObjectValid(t *testing.T) {
	sut := createNodeIdIndex()
	node := makeTestNode("id")
	ok, val, err := sut.FromObject(node)
	assert.True(t, ok)
	assert.Equal(t, []byte{'i', 'd', 0}, val)
	assert.Nil(t, err)
}

type schedulingResource struct {
	cpu     int
	memory  int
	storage int
}

func TestIteration(t *testing.T) {
	nodes := make([]schedulingResource, 0, 10000)
	for i := 0; i < 10000; i++ {
		nodes = append(nodes, schedulingResource{
			cpu:     rand.Intn(30),
			memory:  rand.Intn(100000000),
			storage: rand.Intn(100000000),
		})
	}

	jobs := make([]schedulingResource, 0, 10000)
	for i := 0; i < 10000; i++ {
		jobs = append(jobs, schedulingResource{
			cpu:     rand.Intn(30),
			memory:  rand.Intn(100000000),
			storage: rand.Intn(100000000),
		})
	}

	start := time.Now()

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].cpu < nodes[j].cpu {
			return true
		}
		if nodes[i].memory < nodes[j].memory {
			return true
		}
		return nodes[i].storage < nodes[j].storage
	})

	schedulable := 0
	nonSchedulable := 0

	startLoop := time.Now()
	for _, job := range jobs {
		for _, node := range nodes {
			if node.cpu >= job.cpu && node.memory >= job.memory && node.storage >= job.storage {
				schedulable++
				break
			} else {
				nonSchedulable++
			}
		}
	}
	copyStart := time.Now()
	nodesCopy := make([]schedulingResource, len(nodes))

	_ = copy(nodesCopy, nodes) // s2 is now an independent copy of s1

	fmt.Println(fmt.Sprintf("%d schedulable, %d non schedulable", schedulable, nonSchedulable))
	fmt.Println(fmt.Sprintf("total: %s, loop: %s, copy: %s", time.Now().Sub(start), time.Now().Sub(startLoop), time.Now().Sub(copyStart)))
}

func TestFromObjectEmptyId(t *testing.T) {
	sut := createNodeIdIndex()
	node := makeTestNode("")
	ok, val, err := sut.FromObject(node)
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.Nil(t, err)
}

func TestFromObjectWrongType(t *testing.T) {
	sut := createNodeIdIndex()
	ok, val, err := sut.FromObject("this should not be a string")
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.NotNil(t, err)
}

func TestFromArgsValid(t *testing.T) {
	sut := createNodeIdIndex()
	val, err := sut.FromArgs("id")
	assert.Equal(t, []byte{'i', 'd', 0}, val)
	assert.Nil(t, err)
}

func makeTestNode(id string) *internaltypes.Node {
	return internaltypes.CreateNode(id,
		internaltypes.NewNodeType([]v1.Taint{},
			map[string]string{},
			map[string]bool{},
			map[string]bool{},
		),
		1,
		"executor",
		"node_name",
		"pool",
		[]v1.Taint{},
		map[string]string{},
		internaltypes.ResourceList{},
		map[int32]internaltypes.ResourceList{},
		map[string]internaltypes.ResourceList{},
		map[string]internaltypes.ResourceList{},
		map[string]bool{},
		[][]byte{},
	)
}

package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration"
	fakeContext "github.com/armadaproject/armada/internal/executor/fake/context"
	"github.com/armadaproject/armada/pkg/api"
)

func TestJobLease_GetAvoidNodeLabels_EverythingSetUpCorrectly_ReturnsLabels(t *testing.T) {
	avoidNodeLabels := []string{"a", "c"}

	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node1"}}

	testAppConfig := configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

	node := &fakeContext.NodeSpec{Name: "node1", Count: 1}
	node.Labels = map[string]string{"a": "aa", "b": "bb", "c": "cc"}
	fakeCc := fakeContext.NewFakeClusterContext(testAppConfig, []*fakeContext.NodeSpec{node})

	labels, err := getAvoidNodeLabels(pod, avoidNodeLabels, fakeCc)

	assert.Equal(t, makeOrderedMap(label{name: "a", val: "aa"}, label{name: "c", val: "cc"}), labels)
	assert.Nil(t, err)
}

func TestJobLease_GetAvoidNodeLabels_NodeNameNotSet_ReturnsEmptyMap(t *testing.T) {
	avoidNodeLabels := []string{"a"}

	pod := &v1.Pod{Spec: v1.PodSpec{}}

	testAppConfig := configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

	node := &fakeContext.NodeSpec{Name: "node1", Count: 1}
	node.Labels = map[string]string{"a": "aa"}
	fakeCc := fakeContext.NewFakeClusterContext(testAppConfig, []*fakeContext.NodeSpec{node})

	labels, err := getAvoidNodeLabels(pod, avoidNodeLabels, fakeCc)

	assert.Equal(t, makeOrderedMap(), labels)
	assert.Nil(t, err)
}

func TestJobLease_GetAvoidNodeLabels_NoMatchingLabels_ReturnsError(t *testing.T) {
	avoidNodeLabels := []string{"a"}

	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node1"}}

	testAppConfig := configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

	node := &fakeContext.NodeSpec{Name: "node1", Count: 1}
	node.Labels = map[string]string{"b": "bb"}
	fakeCc := fakeContext.NewFakeClusterContext(testAppConfig, []*fakeContext.NodeSpec{node})

	labels, err := getAvoidNodeLabels(pod, avoidNodeLabels, fakeCc)

	assert.NotNil(t, err)
	assert.Nil(t, labels)
}

func makeOrderedMap(labels ...label) *api.OrderedStringMap {
	entries := []*api.StringKeyValuePair{}
	for _, kv := range labels {
		entries = append(entries, &api.StringKeyValuePair{Key: kv.name, Value: kv.val})
	}
	return &api.OrderedStringMap{Entries: entries}
}

type label struct {
	name string
	val  string
}

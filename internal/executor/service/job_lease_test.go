package service

import (
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/executor/configuration"
	fakeContext "github.com/G-Research/armada/internal/executor/fake/context"
	"github.com/stretchr/testify/assert"
)

func TestJobLease_GetAvoidNodeLabels_EverythingSetUpCorrectly_ReturnsLabels(t *testing.T) {

	avoidNodeLabels := []string{"a", "c"}

	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node1"}}

	var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

	node := &fakeContext.NodeSpec{Name: "node1", Count: 1}
	node.Labels = map[string]string{"a": "aa", "b": "bb", "c": "cc"}
	fakeCc := fakeContext.NewFakeClusterContext(testAppConfig, []*fakeContext.NodeSpec{node})

	labels, err := getAvoidNodeLabels(pod, avoidNodeLabels, fakeCc)

	assert.Equal(t, map[string]string{"a": "aa", "c": "cc"}, labels)
	assert.Nil(t, err)

}

func TestJobLease_GetAvoidNodeLabels_NodeNameNotSet_ReturnsError(t *testing.T) {

	avoidNodeLabels := []string{"a"}

	pod := &v1.Pod{Spec: v1.PodSpec{}}

	var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

	node := &fakeContext.NodeSpec{Name: "node1", Count: 1}
	node.Labels = map[string]string{"a": "aa"}
	fakeCc := fakeContext.NewFakeClusterContext(testAppConfig, []*fakeContext.NodeSpec{node})

	labels, err := getAvoidNodeLabels(pod, avoidNodeLabels, fakeCc)

	assert.NotNil(t, err)
	assert.Nil(t, labels)

}

func TestJobLease_GetAvoidNodeLabels_NoMatchingLabels_ReturnsError(t *testing.T) {

	avoidNodeLabels := []string{"a"}

	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node1"}}

	var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

	node := &fakeContext.NodeSpec{Name: "node1", Count: 1}
	node.Labels = map[string]string{"b": "bb"}
	fakeCc := fakeContext.NewFakeClusterContext(testAppConfig, []*fakeContext.NodeSpec{node})

	labels, err := getAvoidNodeLabels(pod, avoidNodeLabels, fakeCc)

	assert.NotNil(t, err)
	assert.Nil(t, labels)

}

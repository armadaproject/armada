package startup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

const (
	cachedImageTag    = "docker.io/repo/warm:1.0"
	cachedImageDigest = "docker.io/repo/warm@sha256:aaaa"
)

// nodeWithCachedImage reports the image under both its digest and tag, as the kubelet does.
func nodeWithCachedImage(sizeBytes int64) *v1.Node {
	return &v1.Node{Status: v1.NodeStatus{Images: []v1.ContainerImage{
		{Names: []string{cachedImageDigest, cachedImageTag}, SizeBytes: sizeBytes},
		{Names: []string{"docker.io/repo/other@sha256:bbbb", "docker.io/repo/other:1.0"}, SizeBytes: 999},
	}}}
}

func TestNodeImageSizes_MatchesOnImageID(t *testing.T) {
	sizes := newNodeImageSizes(nodeWithCachedImage(1234))

	size := sizes.sizeOf(v1.ContainerStatus{ImageID: cachedImageDigest, Image: "docker.io/repo/unknown:1.0"})

	require.NotNil(t, size)
	assert.Equal(t, uint64(1234), size.Value)
}

func TestNodeImageSizes_FallsBackToImage(t *testing.T) {
	sizes := newNodeImageSizes(nodeWithCachedImage(1234))

	size := sizes.sizeOf(v1.ContainerStatus{ImageID: "sha256:not-reported-by-node", Image: cachedImageTag})

	require.NotNil(t, size)
	assert.Equal(t, uint64(1234), size.Value)
}

func TestNodeImageSizes_NilWhenImageNotOnNode(t *testing.T) {
	sizes := newNodeImageSizes(nodeWithCachedImage(1234))

	assert.Nil(t, sizes.sizeOf(v1.ContainerStatus{ImageID: "docker.io/repo/missing@sha256:cccc", Image: "docker.io/repo/missing:1.0"}))
}

func TestNodeImageSizes_NilWhenNodeReportsNoSize(t *testing.T) {
	sizes := newNodeImageSizes(nodeWithCachedImage(0))

	assert.Nil(t, sizes.sizeOf(v1.ContainerStatus{ImageID: cachedImageDigest, Image: cachedImageTag}))
}

func TestNodeImageSizes_NilWhenNodeUnknown(t *testing.T) {
	sizes := newNodeImageSizes(nil)

	assert.Nil(t, sizes.sizeOf(v1.ContainerStatus{ImageID: cachedImageDigest, Image: cachedImageTag}))
}
